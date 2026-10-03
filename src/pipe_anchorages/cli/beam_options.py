"""Bridges a gfw.common.cli Command's parsed config into an apache_beam PipelineOptions.

Used by the pipelines still built directly on apache_beam.Pipeline / PipelineOptions
rather than the gfw.common.beam.pipeline wrapper -- their own *_pipeline.py modules
are untouched, and still call ``options.view_as(<SomeOptions>)`` to read their flags,
so this has to hand them back a real ``PipelineOptions`` instance, not just a
SimpleNamespace.
"""
from types import SimpleNamespace
from typing import Iterable

from apache_beam.options.pipeline_options import PipelineOptions


def build_pipeline_options(config: SimpleNamespace, fields: Iterable[str]) -> PipelineOptions:
    """Reconstructs a beam PipelineOptions object from a Command's parsed config.

    ``fields`` are this command's own domain-specific option dests. Dataflow/Beam's
    own flags (``--runner``, ``--project``, ``--temp-location``, etc.) aren't declared
    on the Command at all, so the CLI framework leaves them as unknown args, which
    pass through untouched for Beam's own parsing to pick up via ``view_as(...)``.

    ``--labels`` needs special handling: the CLI's shared ``--labels`` option parses
    into a dict (``config.labels``), but ``GoogleCloudOptions.labels`` is a Beam-native
    repeated flag, so this re-encodes it as repeated ``--labels=key=value`` tokens.
    """
    flags: list[str] = []

    for field in fields:
        value = getattr(config, field, None)

        if value is None:
            continue

        if isinstance(value, bool):
            if value:
                flags.append(f"--{field}")
            continue

        flags.append(f"--{field}={value}")

    for key, value in (config.labels or {}).items():
        flags.append(f"--labels={key}={value}")

    flags.extend(config.unknown_unparsed_args)

    return PipelineOptions(flags=flags)
