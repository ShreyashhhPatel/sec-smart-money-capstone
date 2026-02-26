"""
======================================================================================
⚙️ PHASE 4.4 — ORCHESTRATION ENGINE (RESILIENT TASK EXECUTOR)
======================================================================================
Core orchestration logic with dependency management, retries, and conditional execution.
Replaces manual notebook orchestration with intelligent workflow.
======================================================================================
"""

import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable, Optional, Tuple
from enum import Enum
import json

class TaskDependencyError(Exception):
    """Raised when task dependencies cannot be satisfied"""
    pass

class CriticalTaskFailure(Exception):
    """Raised when a critical task fails and recovery is not possible"""
    pass


class TaskExecutionEngine:
    """
    Intelligent task executor with:
    - Dependency resolution
    - Conditional execution
    - Automatic retries
    - Error recovery
    - Progress tracking
    """
    
    def __init__(self, spark, audit_logger, config, quality_validator=None):
        self.spark = spark
        self.audit_logger = audit_logger
        self.config = config
        self.quality_validator = quality_validator
        
        self.execution_order = {}  # task_name -> order
        self.task_results = {}     # task_name -> {"status": ..., "output": ...}
        self.task_metadata = {}    # task_name -> config
        self.execution_times = {}  # task_name -> duration_seconds
        
    def register_task(self, task_name: str, task_config: Dict[str, Any]) -> None:
        """Register a task in the execution plan"""
        self.task_metadata[task_name] = task_config
    
    def validate_dependency_graph(self, tasks: Dict[str, Dict]) -> bool:
        """
        Validate task dependency graph for:
        - Circular dependencies
        - Non-existent dependencies
        - Proper ordering
        """
        all_tasks = set(tasks.keys())
        visited = set()
        rec_stack = set()
        
        def has_cycle(task, visited, rec_stack):
            visited.add(task)
            rec_stack.add(task)
            
            depends_on = tasks.get(task, {}).get('depends_on', [])
            for dep in depends_on:
                if dep not in visited:
                    if has_cycle(dep, visited, rec_stack):
                        return True
                elif dep in rec_stack:
                    return True
            
            rec_stack.remove(task)
            return False
        
        # Check for cycles
        for task in all_tasks:
            if task not in visited:
                if has_cycle(task, visited, rec_stack):
                    raise TaskDependencyError(f"Circular dependency detected involving {task}")
        
        # Check for non-existent dependencies
        for task, config in tasks.items():
            for dep in config.get('depends_on', []):
                if dep not in all_tasks:
                    raise TaskDependencyError(
                        f"Task {task} depends on non-existent task {dep}"
                    )
        
        return True
    
    def topological_sort(self, tasks: Dict[str, Dict]) -> List[str]:
        """
        Topologically sort tasks by dependencies.
        Returns list of task names in execution order.
        """
        self.validate_dependency_graph(tasks)
        
        in_degree = {task: 0 for task in tasks}
        adjacency = {task: [] for task in tasks}
        
        # Build graph
        for task, config in tasks.items():
            for dep in config.get('depends_on', []):
                adjacency[dep].append(task)
                in_degree[task] += 1
        
        # Kahn's algorithm
        queue = [task for task in tasks if in_degree[task] == 0]
        result = []
        
        while queue:
            task = queue.pop(0)
            result.append(task)
            
            for neighbor in adjacency[task]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Record execution order
        for idx, task in enumerate(result, 1):
            self.execution_order[task] = idx
        
        return result
    
    def should_skip_task(self, task_name: str, skip_condition: Optional[str]) -> bool:
        """
        Evaluate if a task should be skipped based on condition.
        Example condition: "if_no_new_files" or "if_not_updated"
        """
        if not skip_condition:
            return False
        
        if skip_condition == "if_no_new_files":
            # Check watermark table for new data
            try:
                result = self.spark.sql(f"""
                SELECT COUNT(*) as new_files
                FROM fintech_analytics.audit.watermarks
                WHERE last_processed_date < CURRENT_DATE()
                """).collect()
                return result[0]['new_files'] == 0
            except:
                return False
        
        return False
    
    def execute_notebook(self, notebook_path: str, parameters: Dict[str, Any],
                        timeout_seconds: int = 1800) -> Dict[str, Any]:
        """
        Execute a Databricks notebook with parameters.
        Simulated - in real implementation, use dbutils.notebook.run()
        """
        print(f"    📓 Running notebook: {notebook_path}")
        
        # In actual Databricks environment:
        # result = dbutils.notebook.run(notebook_path, timeout_seconds, parameters)
        
        return {
            "status": "SUCCESS",
            "output": "Notebook executed",
            "rows_processed": 1000
        }
    
    def execute_sql(self, sql_query: str, timeout_seconds: int = 600) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        print(f"    🔍 Running SQL query")
        
        try:
            result = self.spark.sql(sql_query)
            row_count = result.count()
            
            return {
                "status": "SUCCESS",
                "rows_affected": row_count,
                "message": f"Query executed, {row_count} rows"
            }
        except Exception as e:
            raise Exception(f"SQL execution failed: {str(e)}")
    
    def execute_python(self, python_func: Callable, 
                      params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Python function"""
        print(f"    🐍 Running Python function")
        
        try:
            result = python_func(self.spark, **params)
            return {
                "status": "SUCCESS",
                "result": result
            }
        except Exception as e:
            raise Exception(f"Python execution failed: {str(e)}")
    
    def execute_task_with_retries(self, task_name: str, task_config: Dict[str, Any],
                                 executor: Callable, max_retries: int = 2) -> Tuple[bool, Dict]:
        """
        Execute task with automatic retry logic.
        Implements exponential backoff between retries.
        """
        attempt = 0
        last_error = None
        
        while attempt <= max_retries:
            attempt += 1
            
            try:
                # Log start
                self.audit_logger.log_task_start(
                    task_name=task_name,
                    task_type=task_config.get('task_type', 'unknown'),
                    execution_order=self.execution_order.get(task_name, 0),
                    attempt=attempt
                )
                
                # Execute
                start_time = time.time()
                result = executor()
                duration = time.time() - start_time
                
                # Log success
                rows_processed = result.get('rows_processed', 0)
                self.audit_logger.log_task_success(
                    task_name=task_name,
                    rows_processed=rows_processed
                )
                
                self.execution_times[task_name] = duration
                
                return True, {
                    "status": "SUCCESS",
                    "attempt": attempt,
                    "duration": duration,
                    "result": result
                }
                
            except Exception as e:
                last_error = e
                error_trace = traceback.format_exc()
                
                print(f"    ⚠️  Attempt {attempt} failed: {type(e).__name__}")
                
                if attempt < max_retries + 1:
                    # Exponential backoff
                    wait_seconds = 2 ** (attempt - 1)
                    print(f"    ⏳ Retrying in {wait_seconds} seconds...")
                    time.sleep(wait_seconds)
                else:
                    # Final attempt failed
                    self.audit_logger.log_task_failure(
                        task_name=task_name,
                        error=e,
                        stack_trace=error_trace
                    )
                    
                    return False, {
                        "status": "FAILED",
                        "attempt": attempt,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "stack_trace": error_trace
                    }
        
        return False, {
            "status": "FAILED",
            "error": last_error,
            "message": f"Task failed after {max_retries + 1} attempts"
        }
    
    def execute_pipeline(self, tasks: Dict[str, Dict], 
                        executors: Dict[str, Callable],
                        run_mode: str = "full") -> Tuple[bool, Dict[str, Any]]:
        """
        Execute entire pipeline with dependency resolution.
        
        Args:
            tasks: {task_name: {depends_on: [...], ...}}
            executors: {task_name: callable}
            run_mode: "full" | "incremental" | "repair"
        
        Returns:
            (success: bool, summary: dict)
        """
        print("\n" + "="*80)
        print(f"🚀 STARTING PIPELINE EXECUTION ({run_mode.upper()})")
        print("="*80)
        
        # 1. Validate and sort
        try:
            execution_plan = self.topological_sort(tasks)
            print(f"\n📋 Execution plan ({len(execution_plan)} tasks):")
            for idx, task in enumerate(execution_plan, 1):
                deps = tasks[task].get('depends_on', [])
                deps_str = f" ← {','.join(deps)}" if deps else ""
                print(f"  {idx}. {task}{deps_str}")
        except TaskDependencyError as e:
            print(f"\n❌ Dependency error: {e}")
            return False, {"error": str(e)}
        
        # 2. Execute tasks
        summary = {
            "total_tasks": len(execution_plan),
            "successful_tasks": 0,
            "failed_tasks": 0,
            "skipped_tasks": 0,
            "task_results": {},
            "failed_tasks_list": []
        }
        
        for task_name in execution_plan:
            task_config = tasks[task_name]
            
            # Check skip condition
            skip_condition = task_config.get('skip_on_condition')
            if self.should_skip_task(task_name, skip_condition):
                self.audit_logger.log_task_skip(task_name, skip_condition)
                self.task_results[task_name] = {"status": "SKIPPED"}
                summary["skipped_tasks"] += 1
                continue
            
            # Check dependencies
            can_execute = True
            for dep in task_config.get('depends_on', []):
                if self.task_results.get(dep, {}).get('status') != 'SUCCESS':
                    can_execute = False
                    print(f"\n⚠️  {task_name} cannot execute: dependency {dep} failed")
                    break
            
            if not can_execute:
                if task_config.get('critical', True):
                    summary["failed_tasks"] += 1
                    summary["failed_tasks_list"].append(task_name)
                    raise CriticalTaskFailure(f"Critical task {task_name} blocked by failed dependency")
                else:
                    self.audit_logger.log_task_skip(task_name, "dependency_failed")
                    summary["skipped_tasks"] += 1
                    continue
            
            # Execute with retries
            executor = executors.get(task_name)
            if not executor:
                print(f"❌ No executor found for {task_name}")
                summary["failed_tasks"] += 1
                summary["failed_tasks_list"].append(task_name)
                continue
            
            success, result = self.execute_task_with_retries(
                task_name=task_name,
                task_config=task_config,
                executor=executor,
                max_retries=task_config.get('max_retries', 2)
            )
            
            self.task_results[task_name] = result
            summary["task_results"][task_name] = result
            
            if success:
                summary["successful_tasks"] += 1
            else:
                summary["failed_tasks"] += 1
                summary["failed_tasks_list"].append(task_name)
                
                # Stop if critical
                if task_config.get('critical', True):
                    raise CriticalTaskFailure(f"Critical task {task_name} failed")
        
        # 3. Final status
        overall_success = summary["failed_tasks"] == 0
        overall_status = "SUCCESS" if overall_success else "FAILED"
        
        error_summary = f"Failed tasks: {', '.join(summary['failed_tasks_list'])}" \
            if summary['failed_tasks_list'] else ""
        
        self.audit_logger.end_run(
            overall_status=overall_status,
            successful_tasks=summary["successful_tasks"],
            failed_tasks=summary["failed_tasks"],
            skipped_tasks=summary["skipped_tasks"],
            error_summary=error_summary
        )
        
        return overall_success, summary


if __name__ == "__main__":
    print("Orchestration engine loaded")
