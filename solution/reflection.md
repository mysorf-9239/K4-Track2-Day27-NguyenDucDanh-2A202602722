# Reflection (≤1 page)

## Which fault types were hardest to catch, and why?

The hardest cases were the subtle distribution and drift faults. A single
baseline bound is reliable for large changes, but it can miss an event whose
individual values are still close to normal. For batch checks, I therefore
combined row count, mean amount, null rate, and staleness into a normalized
multi-signal distance. For feature drift and embedding drift, I used the
separation ranges described in the lab instead of treating the published
3-sigma baseline as a perfect decision boundary.

## What would you change about your cost/coverage tradeoff?

Each event uses its one matching metered tool, so the detector keeps complete
visibility without duplicate calls. This costs 180 credits in Practice and
240 credits in Public; Public exceeds its 220-credit budget, but the observed
TPR and zero FPR make that tradeoff worthwhile. On another pass, I would use
the free in-memory state to maintain robust rolling summaries per event type,
then skip or prioritize expensive checks only when the event metadata gives a
strong indication that it is ordinary.

The current verification results were Practice score 50.0 with TPR 1.0 and
FPR 0.0, Public score 46.9 with TPR 0.9744 and FPR 0.0, and Private score
32.41 with TPR 0.6481, FPR 0.0, and no cost overage.
