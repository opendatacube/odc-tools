from timeit import default_timer as t_now
from types import SimpleNamespace


class RateEstimator:
    def __init__(self):
        self.t0 = t_now()
        self.t_last = self.t0
        self.n = 0

    def _compute(self):
        dt = self.t_last - self.t0
        fps = 0 if self.n == 0 else self.n / dt
        return SimpleNamespace(elapsed=dt, n=self.n, fps=fps)

    def stats(self):
        return self._compute()

    def every(self, N):
        return (self.n % N) == 0

    def __call__(self, n=1):
        self.t_last = t_now()
        self.n += n

    def __str__(self):
        state = self._compute()
        return "N: {s.n:6,d} T: {s.elapsed:6.1f}s FPS: {s.fps:4.1f}".format(s=state)

    def __repr__(self):
        return "<t0:{}, t_last:{}, n:{}>".format(self.t0, self.t_last, self.n)
