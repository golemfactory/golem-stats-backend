"""Utilities for logging computation events via the standard `logging` module.

Functions in this module fall into two categories:

* Functions that convert computation events generated by the `Executor.submit`
method to calls to the standard Python `logging` module, using the loggers
in the "yapapi" namespace (e.g. `logging.getLogger("yapapi.executor")`).
These functions should be passed as `event_consumer` arguments to `Executor()`.

* Functions that perform configuration of the `logging` module itself.
Since logging configuration is in general a responsibility of the code that
uses `yapapi` as a library, we only provide the `enable_default_logger`
function in this category, that enables logging to stderr with level `logging.INFO`
and, optionally, to a given file with level `logging.DEBUG`.


Functions for handling events
-----------------------------

Several functions from this module can be passed as `event_consumer` callback to
`yapapi.Executor()`.

For detailed, human-readable output use the `log_event` function:
```python
    Executor(..., event_consumer=yapapi.log.log_event)
```
For even more detailed, machine-readable output use `log_event_repr`:
```python
    Executor(..., event_consumer=yapapi.log.log_event_repr)
```
For summarized, human-readable output use `log_summary()`:
```python
    Executor(..., event_consumer=yapapi.log.log_summary())
```
Summary output can be combined with a detailed one by passing the detailed logger
as an argument to `log_summary`:
```python
    Executor(
        ...
        event_consumer=yapapi.log.log_summary(yapapi.log.log_event_repr)
    )
```
"""
from asyncio import CancelledError
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import itertools
import logging
import os
import sys
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import yapapi.executor.events as events
from yapapi import __version__ as yapapi_version

event_logger = logging.getLogger("yapapi.events")
executor_logger = logging.getLogger("yapapi.executor")

# Initializing loggers, so that logger.setLevel() in enable_default_logger will work.
_agreements_pool_logger = logging.getLogger("yapapi.agreements_pool")


def enable_default_logger(
    format_: str = "[%(asctime)s %(levelname)s %(name)s] %(message)s",
    log_file: Optional[str] = None,
    debug_activity_api: bool = False,
    debug_market_api: bool = False,
    debug_payment_api: bool = False,
):
    """Enable the default logger that logs messages to stderr with level `INFO`.

    If `log_file` is specified, the logger with output messages with level `DEBUG` to
    the given file.
    """
    logger = logging.getLogger("yapapi")
    logger.setLevel(logging.DEBUG)
    logger.disabled = False
    formatter = logging.Formatter(format_)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(filename=log_file, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        logger.debug(
            "Yapapi version: %s, script: %s, working directory: %s",
            yapapi_version,
            sys.argv[0],
            os.getcwd(),
        )
        logger.info(
            "Using log file `%s`; in case of errors look for additional information there", log_file
        )

        for flag, logger_name in (
            (debug_activity_api, "ya_activity"),
            (debug_market_api, "ya_market"),
            (debug_payment_api, "ya_payment"),
        ):
            if flag:
                api_logger = logging.getLogger(logger_name)
                api_logger.setLevel(logging.DEBUG)
                api_logger.addHandler(file_handler)


# Default human-readable representation of event types.
event_type_to_string = {
    events.ComputationStarted: "Computation started",
    events.ComputationFinished: "Computation finished",
    events.SubscriptionCreated: "Demand published on the market",
    events.SubscriptionFailed: "Demand publication failed",
    events.CollectFailed: "Failed to collect proposals for demand",
    events.NoProposalsConfirmed: "No proposals confirmed by providers",
    events.ProposalReceived: "Proposal received from the market",
    events.ProposalRejected: "Proposal rejected",
    events.ProposalResponded: "Responded to a proposal",
    events.ProposalFailed: "Failed to respond to proposal",
    events.ProposalConfirmed: "Proposal confirmed by provider",
    events.AgreementCreated: "Agreement proposal sent to provider",
    events.AgreementConfirmed: "Agreement approved by provider",
    events.AgreementRejected: "Agreement rejected by provider",
    events.AgreementTerminated: "Agreement terminated",
    events.DebitNoteReceived: "Debit note received",
    events.PaymentAccepted: "Payment accepted",
    events.PaymentFailed: "Payment failed",
    events.PaymentPrepared: "Payment prepared",
    events.PaymentQueued: "Payment queued",
    events.InvoiceReceived: "Invoice received",
    events.WorkerStarted: "Worker started for agreement",
    events.ActivityCreated: "Activity created on provider",
    events.ActivityCreateFailed: "Failed to create activity",
    events.TaskStarted: "Task started",
    events.ScriptSent: "Script sent to provider",
    events.CommandStarted: "Command started",
    events.CommandStdOut: "Command stdout",
    events.CommandStdErr: "Command stderr",
    events.CommandExecuted: "Script command executed",
    events.GettingResults: "Getting script results",
    events.ScriptFinished: "Script finished",
    events.TaskAccepted: "Task accepted",
    events.TaskRejected: "Task rejected",
    events.WorkerFinished: "Worker finished",
    events.DownloadStarted: "Download started",
    events.DownloadFinished: "Download finished",
    events.ShutdownFinished: "Shutdown finished",
}


def _check_event_type_to_string():
    # This is to check that `event_type_to_string` covers all event types

    _event_types = set(
        member
        for member in events.__dict__.values()
        if type(member) is type and issubclass(member, events.Event)
    )

    _event_types_superclasses = {ty for ev in _event_types for ty in ev.mro() if ty is not ev}

    _concrete_event_types = _event_types.difference(_event_types_superclasses)

    assert len(_concrete_event_types) > 0  # Sanity check

    assert _concrete_event_types.issubset(
        event_type_to_string.keys()
    ), _concrete_event_types.difference(event_type_to_string.keys())


_check_event_type_to_string()


def log_event(event: events.Event) -> None:
    """Log `event` with a human-readable description."""

    loglevel = logging.DEBUG

    if not event_logger.isEnabledFor(loglevel):
        return

    exc_info, _ = event.extract_exc_info()
    descr = event_type_to_string[type(event)]
    msg = "; ".join([descr, *(f"{name} = {value}" for name, value in event.__dict__.items())])
    event_logger.log(loglevel, msg, exc_info=exc_info)


def log_event_repr(event: events.Event) -> None:
    """Log the result of calling `repr(event)`."""

    exc_info, _ = event.extract_exc_info()
    event_logger.debug("%r", event, exc_info=exc_info)


@dataclass(frozen=True)
class ProviderInfo:
    id: str
    name: str
    subnet_tag: Optional[str]


class SummaryLogger:
    """Aggregates information from computation events to provide a high-level summary.

    The logger's `log()` method can be used as `event_consumer` callback
    to `Executor()`. It will aggregate the events generated by `Executor.submit()`
    and output some summary information.

    The optional `wrapped_emitter` argument can be used for chaining event
    emitters: each event logged with `log()` is first passed to
    `wrapped_emitter`.

    For example, with the following setup, each event emitted by `executor`
    will be logged by `log_event_repr`, and additionally, certain events
    will cause summary messages to be logged.
    ```python
        detailed_logger = log_event_repr
        summary_logger = SummaryLogger(wrapped_emitter=detailed_logger).log
        executor = Executor(..., event_consumer=summary_logger)
    ```
    """

    logger = logging.getLogger("yapapi.summary")

    # Generates subsequent numbers, for use in generated provider names
    numbers: Iterator[int]

    # Start time of the computation
    start_time: float

    # Maps received proposal ids to provider ids
    received_proposals: Dict[str, str]

    # Set of confirmed proposal ids
    confirmed_proposals: Set[str]

    # Maps agreement ids to provider infos
    agreement_provider_info: Dict[str, ProviderInfo]

    # Set of agreements confirmed by providers
    confirmed_agreements: Set[str]

    # Maps task id to task data
    task_data: Dict[str, Any]

    # Maps a provider info to the list of task ids computed by the provider
    provider_tasks: Dict[ProviderInfo, List[str]]

    # Map a provider info to the sum of amounts in this provider's invoices
    provider_cost: Dict[ProviderInfo, Decimal]

    # Count how many times a worker failed on a provider
    provider_failures: Dict[ProviderInfo, int]

    # Has computation been cancelled?
    cancelled: bool

    # Has computation finished?
    finished: bool

    # Total time waiting for the first proposal
    time_waiting_for_proposals: timedelta

    def __init__(self, wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
        """Create a SummaryLogger."""

        self._wrapped_emitter = wrapped_emitter
        self.numbers: Iterator[int] = itertools.count(1)
        self.provider_cost = {}
        self._reset()

    def _reset(self) -> None:
        """Reset all information aggregated by this logger related to a single computation.

        Here "computation" means an interval of time between the events
        `ComputationStarted` and `ComputationFinished`.

        Note that the `provider_cost` is not reset here, it is zeroed on `ExecutorShutdown`.
        """

        self.start_time = time.time()
        self.received_proposals = {}
        self.confirmed_proposals = set()
        self.agreement_provider_info = {}
        self.confirmed_agreements = set()
        self.task_data = {}
        self.provider_tasks = defaultdict(list)
        self.provider_failures = Counter()
        self.cancelled = False
        self.finished = False
        self.error_occurred = False
        self.time_waiting_for_proposals = timedelta(0)

    def _print_summary(self) -> None:
        """Print a summary at the end of computation."""

        num_providers = len(
            {self.agreement_provider_info[agr_id] for agr_id in self.confirmed_agreements}
        )
        self.logger.info(
            "Negotiated %d agreements with %d providers",
            len(self.confirmed_agreements),
            num_providers,
        )
        for info, tasks in self.provider_tasks.items():
            self.logger.info("Provider '%s' computed %s", info.name, pluralize(len(tasks), "task"))
        for info in set(self.agreement_provider_info.values()):
            if info not in self.provider_tasks:
                self.logger.info("Provider '%s' did not compute any tasks", info.name)
        for info, num in self.provider_failures.items():
            self.logger.info(
                "Activity failed %s on provider '%s'", pluralize(num, "time"), info.name
            )

    def _print_total_cost(self, partial: bool = False) -> None:
        """Print the sum of all accepted invoices."""

        total_cost = sum(self.provider_cost.values(), Decimal(0))
        label = "Total cost" if not partial else "The cost so far"
        self.logger.info("%s: %s", label, total_cost.normalize())

    def log(self, event: events.Event) -> None:
        """Register an event."""

        if self._wrapped_emitter:
            self._wrapped_emitter(event)

        if self.error_occurred:
            return

        try:
            self._handle(event)
        except Exception:
            self.logger.exception("SummaryLogger entered invalid state")
            self.error_occurred = True

    def _handle(self, event: events.Event):
        if isinstance(event, events.ComputationStarted):
            self._reset()
            if self.provider_cost:
                # This means another computation run in the current Executor instance.
                self._print_total_cost(partial=True)
            timeout = event.expires - datetime.now(timezone.utc)
            if not timedelta(minutes=5, seconds=5) <= timeout <= timedelta(minutes=30):
                min, sec = divmod(round(timeout.total_seconds()), 60)
                self.logger.warning(
                    f"Expiration time for your tasks is set to {min} min {sec} sec from now."
                    " Providers will probably not respond to tasks which expire sooner than 5 min"
                    " or later than 30 min, counting from the moment they get your demand."
                    " Use the `timeout` parameter to `Executor()` to adjust the timeout."
                )

        elif isinstance(event, events.ProposalReceived):
            self.received_proposals[event.prop_id] = event.provider_id

        elif isinstance(event, events.ProposalConfirmed):
            self.confirmed_proposals.add(event.prop_id)
            confirmed_providers = set(
                self.received_proposals[prop_id] for prop_id in self.confirmed_proposals
            )
            self.logger.info(
                "Received proposals from %s so far", pluralize(len(confirmed_providers), "provider")
            )

        elif isinstance(event, events.NoProposalsConfirmed):
            self.time_waiting_for_proposals += event.timeout
            if event.num_offers == 0:
                msg = (
                    "No offers have been collected from the market for"
                    f" {self.time_waiting_for_proposals.seconds}s."
                )
            else:
                msg = (
                    f"{event.num_offers} {'offer has' if event.num_offers == 1 else 'offers have'} "
                    f"been collected from the market, but no provider has responded for "
                    f"{self.time_waiting_for_proposals.seconds}s."
                )
            msg += (
                " Make sure you're using the latest released versions of yagna and yapapi,"
                " and the correct subnet. Also make sure that the timeout for computing all"
                " tasks is within the 5 min to 30 min range."
            )
            self.logger.warning(msg)

        elif isinstance(event, events.AgreementCreated):
            provider_name = event.provider_info.name or event.provider_id
            self.logger.info("Agreement proposed to provider '%s'", provider_name)
            self.agreement_provider_info[event.agr_id] = ProviderInfo(
                event.provider_id, provider_name, event.provider_info.subnet_tag
            )

        elif isinstance(event, events.AgreementConfirmed):
            self.logger.info(
                "Agreement confirmed by provider '%s'",
                self.agreement_provider_info[event.agr_id].name,
            )
            self.confirmed_agreements.add(event.agr_id)

        elif isinstance(event, events.TaskStarted):
            self.task_data[event.task_id] = event.task_data

        elif isinstance(event, events.ScriptSent):
            provider_info = self.agreement_provider_info[event.agr_id]
            data = self.task_data[event.task_id] if event.task_id else "<initialization>"
            self.logger.info(
                "Task sent to provider '%s', task data: %s",
                provider_info.name,
                str_capped(data, 200),
            )

        elif isinstance(event, events.ScriptFinished):
            provider_info = self.agreement_provider_info[event.agr_id]
            data = self.task_data[event.task_id] if event.task_id else "<initialization>"
            self.logger.info(
                "Task computed by provider '%s', task data: %s",
                provider_info.name,
                str_capped(data, 200),
            )
            if event.task_id:
                self.provider_tasks[provider_info].append(event.task_id)

        elif isinstance(event, events.PaymentAccepted):
            provider_info = self.agreement_provider_info[event.agr_id]
            cost = self.provider_cost.get(provider_info, Decimal(0))
            cost += Decimal(event.amount)
            self.provider_cost[provider_info] = cost
            self.logger.info(
                "Accepted invoice from '%s', amount: %s", provider_info.name, cost.normalize()
            )

        elif isinstance(event, events.PaymentFailed):
            assert event.exc_info
            _exc_type, exc, _tb = event.exc_info
            provider_info = self.agreement_provider_info[event.agr_id]
            reason = str(exc) or repr(exc) or "unexpected error"
            self.logger.error(
                "Failed to accept invoice from '%s', reason: %s", provider_info, reason
            )

        elif isinstance(event, events.WorkerFinished):
            if event.exc_info is None or self.cancelled:
                return
            _exc_type, exc, _tb = event.exc_info
            provider_info = self.agreement_provider_info[event.agr_id]
            self.provider_failures[provider_info] += 1
            reason = str(exc) or repr(exc) or "unexpected error"
            self.logger.warning(
                "Activity failed on provider '%s', reason: %s", provider_info.name, reason
            )

        elif isinstance(event, events.ComputationFinished):
            if not event.exc_info:
                total_time = time.time() - self.start_time
                self.logger.info(f"Computation finished in {total_time:.1f}s")
                self.finished = True
            else:
                _exc_type, exc, _tb = event.exc_info
                if isinstance(exc, CancelledError):
                    self.cancelled = True
                    self.logger.warning("Computation cancelled")
                else:
                    reason = str(exc) or repr(exc) or "unexpected error"
                    self.logger.error(f"Computation failed, reason: %s", reason)
            self._print_summary()

        elif isinstance(event, events.ShutdownFinished):
            self._print_total_cost()
            self.provider_cost = {}
            if not event.exc_info:
                self.logger.info(f"Executor has shut down")
            else:
                _exc_type, exc, _tb = event.exc_info
                reason = str(exc) or repr(exc) or "unexpected error"
                self.logger.error("Error when shutting down Executor: %s", reason)


def log_summary(wrapped_emitter: Optional[Callable[[events.Event], None]] = None):
    """Output a summary of computation.

    This is a utility function that creates a `SummaryLogger` instance
    wrapping an optional `wrapped_emitter` and returns its `log` method.

    See the documentation of `SummaryLogger` for more information.
    """
    summary_logger = SummaryLogger(wrapped_emitter)
    return summary_logger.log


def pluralize(num: int, thing: str) -> str:
    """Return the string f"1 {thing}" or f"{num} {thing}s", depending on `num`."""
    return f"1 {thing}" if num == 1 else f"{num} {thing}s"


def str_capped(object: Any, max_len: int) -> str:
    """Return the string representation of `object` trimmed to `max_len`.

    Trailing ellipsis is added to the returned string if the original had to be trimmed.
    """
    s = str(object)
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..." if max_len >= 3 else "..."
