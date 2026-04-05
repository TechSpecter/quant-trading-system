from __future__ import annotations

from typing import Callable, List, Any, Dict, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed


def _get_max_workers(config: Dict[str, Any]) -> int:
    md_cfg = config.get("market_data", {})
    chunk_cfg = md_cfg.get("chunking", {})
    workers = chunk_cfg.get("max_parallel_requests", 5)

    try:
        workers_int = int(workers)
        return max(1, workers_int)
    except Exception:
        return 5


def _execute_task(func: Callable[..., Any], args: Any) -> Any:
    try:
        if isinstance(args, tuple):
            return func(*args)
        return func(args)
    except Exception:
        return None


def _submit_tasks(
    executor: ThreadPoolExecutor,
    func: Callable[..., Any],
    tasks: Iterable[Any],
):
    futures = []
    for task in tasks:
        futures.append(executor.submit(_execute_task, func, task))
    return futures


def _collect_results(futures) -> List[Any]:
    results: List[Any] = []
    for future in as_completed(futures):
        try:
            result = future.result()
            if result is not None:
                results.append(result)
        except Exception:
            continue
    return results


def run_parallel(
    func: Callable[..., Any],
    tasks: List[Any],
    config: Dict[str, Any],
) -> List[Any]:
    """
    Public API

    Executes tasks in parallel using ThreadPoolExecutor.
    - func: function to execute
    - tasks: list of inputs (tuple or single arg)
    - config: controls max_parallel_requests
    """

    if not tasks:
        return []

    max_workers = _get_max_workers(config)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = _submit_tasks(executor, func, tasks)
        results = _collect_results(futures)

    return results
