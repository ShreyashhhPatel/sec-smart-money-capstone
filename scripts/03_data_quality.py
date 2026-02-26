"""
======================================================================================
✅ PHASE 4.3 — DATA QUALITY FRAMEWORK
======================================================================================
Contract-based validation, schema enforcement, and quality gates.
======================================================================================
"""

from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class CheckStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"

@dataclass
class QualityCheckResult:
    """Result of a quality check"""
    check_name: str
    table_name: str
    status: CheckStatus
    message: str
    expected: Any = None
    actual: Any = None
    severity: str = "ERROR"  # ERROR | WARNING


class DataQualityValidator:
    """Validates data against contracts and rules"""
    
    def __init__(self, spark, audit_logger=None):
        self.spark = spark
        self.audit_logger = audit_logger
        self.checks_passed = 0
        self.checks_failed = 0
        
    def validate_table_exists(self, table_name: str) -> QualityCheckResult:
        """Check if table exists"""
        try:
            count = self.spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
            status = CheckStatus.PASS
            message = f"Table exists with {count} rows"
        except:
            status = CheckStatus.FAIL
            message = f"Table {table_name} does not exist"
            count = 0
        
        return QualityCheckResult(
            check_name="table_exists",
            table_name=table_name,
            status=status,
            message=message,
            actual=count
        )
    
    def validate_required_columns(self, table_name: str, 
                                 required_cols: List[str]) -> QualityCheckResult:
        """Verify all required columns exist"""
        try:
            df = self.spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
            actual_cols = set(df.columns)
            required_set = set(required_cols)
            missing = required_set - actual_cols
            
            if not missing:
                status = CheckStatus.PASS
                message = f"All {len(required_cols)} required columns present"
            else:
                status = CheckStatus.FAIL
                message = f"Missing columns: {', '.join(missing)}"
            
            return QualityCheckResult(
                check_name="required_columns",
                table_name=table_name,
                status=status,
                message=message,
                expected=required_cols,
                actual=list(actual_cols)
            )
        except Exception as e:
            return QualityCheckResult(
                check_name="required_columns",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error checking columns: {str(e)}"
            )
    
    def validate_not_null(self, table_name: str, columns: List[str],
                         max_null_percentage: float = 5.0) -> List[QualityCheckResult]:
        """Check for null values in critical columns"""
        results = []
        
        try:
            df = self.spark.sql(f"SELECT * FROM {table_name}")
            total_rows = df.count()
            
            for col in columns:
                null_count = df.filter(f"{col} IS NULL").count()
                null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
                
                if null_percentage <= max_null_percentage:
                    status = CheckStatus.PASS
                else:
                    status = CheckStatus.FAIL
                
                message = f"{col}: {null_percentage:.2f}% nulls ({null_count}/{total_rows} rows)"
                
                results.append(QualityCheckResult(
                    check_name=f"not_null_{col}",
                    table_name=table_name,
                    status=status,
                    message=message,
                    expected=f"<= {max_null_percentage}%",
                    actual=f"{null_percentage:.2f}%"
                ))
        except Exception as e:
            results.append(QualityCheckResult(
                check_name="not_null",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error validating nulls: {str(e)}"
            ))
        
        return results
    
    def validate_row_count(self, table_name: str, min_rows: int = 0,
                          max_rows: Optional[int] = None) -> QualityCheckResult:
        """Validate table has expected row count"""
        try:
            actual_count = self.spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
            
            if actual_count < min_rows:
                status = CheckStatus.FAIL
                message = f"Row count {actual_count} < minimum {min_rows}"
            elif max_rows and actual_count > max_rows:
                status = CheckStatus.FAIL
                message = f"Row count {actual_count} > maximum {max_rows}"
            else:
                status = CheckStatus.PASS
                message = f"Row count {actual_count} within expected range"
            
            return QualityCheckResult(
                check_name="row_count",
                table_name=table_name,
                status=status,
                message=message,
                expected=f"{min_rows} - {max_rows or 'unlimited'}",
                actual=actual_count
            )
        except Exception as e:
            return QualityCheckResult(
                check_name="row_count",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error validating row count: {str(e)}"
            )
    
    def validate_numeric_range(self, table_name: str, column: str,
                              min_value: float, max_value: float) -> QualityCheckResult:
        """Validate numeric column values are within range"""
        try:
            result = self.spark.sql(f"""
            SELECT 
                MIN({column}) as min_val,
                MAX({column}) as max_val,
                COUNT(*) as total_rows,
                SUM(CASE WHEN {column} < {min_value} OR {column} > {max_value} THEN 1 ELSE 0 END) as out_of_range
            FROM {table_name}
            WHERE {column} IS NOT NULL
            """).collect()[0]
            
            out_of_range = result['out_of_range'] or 0
            
            if out_of_range == 0:
                status = CheckStatus.PASS
                message = f"{column}: all values in range [{result['min_val']}, {result['max_val']}]"
            else:
                status = CheckStatus.FAIL
                message = f"{column}: {out_of_range} values outside range [{min_value}, {max_value}]"
            
            return QualityCheckResult(
                check_name=f"numeric_range_{column}",
                table_name=table_name,
                status=status,
                message=message,
                expected=f"[{min_value}, {max_value}]",
                actual=f"[{result['min_val']}, {result['max_val']}]"
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=f"numeric_range_{column}",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error validating numeric range: {str(e)}"
            )
    
    def validate_uniqueness(self, table_name: str, columns: List[str]) -> QualityCheckResult:
        """Check for duplicate key combinations"""
        try:
            col_str = ", ".join(columns)
            result = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT ({col_str})) as unique_keys
            FROM {table_name}
            WHERE {' AND '.join([f'{c} IS NOT NULL' for c in columns])}
            """).collect()[0]
            
            total = result['total_rows']
            unique = result['unique_keys']
            duplicates = total - unique
            
            if duplicates == 0:
                status = CheckStatus.PASS
                message = f"All {unique} key combinations are unique"
            else:
                status = CheckStatus.FAIL
                message = f"{duplicates} duplicate key combinations found"
            
            return QualityCheckResult(
                check_name=f"unique_{','.join(columns)}",
                table_name=table_name,
                status=status,
                message=message,
                expected="0 duplicates",
                actual=duplicates
            )
        except Exception as e:
            return QualityCheckResult(
                check_name=f"unique_{','.join(columns)}",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error validating uniqueness: {str(e)}"
            )
    
    def validate_schema_contract(self, table_name: str, 
                                expected_schema: Dict[str, str]) -> QualityCheckResult:
        """Validate table schema matches contract"""
        try:
            df = self.spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
            actual_schema = {field.name: field.dataType.simpleString() for field in df.schema}
            
            mismatches = []
            for col, expected_type in expected_schema.items():
                if col not in actual_schema:
                    mismatches.append(f"{col} (missing)")
                elif actual_schema[col] != expected_type:
                    mismatches.append(f"{col} (expected {expected_type}, got {actual_schema[col]})")
            
            if not mismatches:
                status = CheckStatus.PASS
                message = f"Schema matches contract ({len(expected_schema)} columns)"
            else:
                status = CheckStatus.FAIL
                message = f"Schema mismatches: {', '.join(mismatches)}"
            
            return QualityCheckResult(
                check_name="schema_contract",
                table_name=table_name,
                status=status,
                message=message,
                expected=expected_schema,
                actual=actual_schema
            )
        except Exception as e:
            return QualityCheckResult(
                check_name="schema_contract",
                table_name=table_name,
                status=CheckStatus.FAIL,
                message=f"Error validating schema: {str(e)}"
            )
    
    def run_all_checks(self, table_name: str, contract: Dict[str, Any],
                      audit_logger=None) -> Tuple[bool, List[QualityCheckResult]]:
        """Run all validation checks for a table"""
        all_results = []
        
        # 1. Table exists
        all_results.append(self.validate_table_exists(table_name))
        
        # 2. Required columns
        if 'required_columns' in contract:
            all_results.append(
                self.validate_required_columns(table_name, contract['required_columns'])
            )
        
        # 3. Not null checks
        if 'null_checks' in contract:
            max_null_pct = contract.get('max_null_percentage', 5.0)
            all_results.extend(
                self.validate_not_null(table_name, contract['null_checks'], max_null_pct)
            )
        
        # 4. Row count
        if 'min_rows' in contract:
            all_results.append(
                self.validate_row_count(table_name, contract['min_rows'])
            )
        
        # 5. Numeric ranges
        if 'numeric_ranges' in contract:
            for col, (min_val, max_val) in contract['numeric_ranges'].items():
                all_results.append(
                    self.validate_numeric_range(table_name, col, min_val, max_val)
                )
        
        # Log results
        all_passed = True
        for result in all_results:
            if audit_logger:
                audit_logger.log_data_quality_check(
                    table_name=result.table_name,
                    check_name=result.check_name,
                    check_type=result.check_name,
                    status=result.status.value,
                    expected=result.expected,
                    actual=result.actual,
                    message=result.message
                )
            
            if result.status != CheckStatus.PASS:
                all_passed = False
                if result.severity == "ERROR":
                    print(f"  ❌ {result.check_name}: {result.message}")
                else:
                    print(f"  ⚠️  {result.check_name}: {result.message}")
            else:
                print(f"  ✅ {result.check_name}: {result.message}")
        
        return all_passed, all_results


if __name__ == "__main__":
    print("Data quality framework loaded")
