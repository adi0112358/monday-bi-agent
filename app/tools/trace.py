from dataclasses import dataclass, asdict
from time import time

@dataclass
class TraceEvent:
    step: str
    detail: str
    rows: int | None = None
    ms: int | None = None

class Tracer:
    def __init__(self):
        self.events = []

    def add(self, step, detail, rows=None, ms=None):
        self.events.append(TraceEvent(step, detail, rows, ms))

    def dump(self):
        return [asdict(e) for e in self.events]

def timed_call(tracer, step, detail, fn):
    t0 = time()
    out = fn()
    ms = int((time() - t0) * 1000)
    rows = len(out) if hasattr(out, "__len__") else None
    tracer.add(step, detail, rows=rows, ms=ms)
    return out
