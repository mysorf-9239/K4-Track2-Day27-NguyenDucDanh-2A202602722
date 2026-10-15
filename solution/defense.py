"""Online fault detector for the Data Siege event stream."""
import math

from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _fault(alert, pillar, reason=""):
    return Verdict(alert=alert, pillar=pillar, reason=reason)


def _tool_failed(result):
    return not isinstance(result, dict) or "error" in result


def check_data_batch(payload, ctx):
    profile = ctx.tools.batch_profile(payload["batch_id"])
    if _tool_failed(profile):
        return _fault(False, "checks")

    baseline = ctx.baseline
    row_count = profile.get("row_count")
    null_rate = profile.get("null_rate", {}).get("customer_id")
    mean_amount = profile.get("mean_amount")
    staleness = profile.get("staleness_min")

    if row_count is None or null_rate is None or mean_amount is None or staleness is None:
        return _fault(False, "checks")

    if not baseline["row_count_min"] <= row_count <= baseline["row_count_max"]:
        return _fault(True, "checks", "row count outside baseline")
    if null_rate > baseline["null_rate_max"]:
        return _fault(True, "checks", "customer_id null rate above baseline")
    if not baseline["mean_amount_min"] <= mean_amount <= baseline["mean_amount_max"]:
        return _fault(True, "checks", "mean amount outside baseline")
    if staleness > baseline["staleness_min_max"]:
        return _fault(True, "checks", "batch staleness above baseline")

    # Catch a conservative multi-signal shift even when every individual
    # value remains just inside its published range.
    row_mid = (baseline["row_count_min"] + baseline["row_count_max"]) / 2.0
    row_half = (baseline["row_count_max"] - baseline["row_count_min"]) / 2.0
    amount_mid = (baseline["mean_amount_min"] + baseline["mean_amount_max"]) / 2.0
    amount_half = (baseline["mean_amount_max"] - baseline["mean_amount_min"]) / 2.0
    distance = math.sqrt(
        ((row_count - row_mid) / row_half) ** 2
        + ((mean_amount - amount_mid) / amount_half) ** 2
        + (null_rate / baseline["null_rate_max"]) ** 2
        + (staleness / baseline["staleness_min_max"]) ** 2
    )
    if distance > 1.3:
        return _fault(True, "checks", "combined batch profile shift")
    return _fault(False, "checks")


def check_contract_checkpoint(payload, ctx):
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if _tool_failed(diff):
        return _fault(False, "contracts")

    violations = diff.get("violations") or []
    if violations:
        return _fault(True, "contracts", "declared contract violation")

    freshness = diff.get("freshness_delay_min")
    if freshness is None:
        return _fault(False, "contracts")
    declared_sla = (payload.get("declared_sla") or {}).get("freshness_min")
    if declared_sla is not None and freshness > declared_sla:
        return _fault(True, "contracts", "freshness exceeds declared SLA")
    if freshness > ctx.baseline["freshness_delay_max_min"]:
        return _fault(True, "contracts", "freshness delay above baseline")
    return _fault(False, "contracts")


def check_lineage_run(payload, ctx):
    graph = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _tool_failed(graph):
        return _fault(False, "lineage")

    duration = graph.get("duration_ms")
    upstream = graph.get("actual_upstream") or []
    downstream_count = graph.get("actual_downstream_count")
    if duration is None or downstream_count is None:
        return _fault(False, "lineage")

    if duration > ctx.baseline["lineage_duration_ms_max"]:
        return _fault(True, "lineage", "lineage runtime above baseline")
    if downstream_count == 0:
        return _fault(True, "lineage", "orphan output")

    required_upstream = {"raw.orders", "raw.customers"}
    if not required_upstream.issubset(set(upstream)):
        return _fault(True, "lineage", "required upstream dependency missing")
    return _fault(False, "lineage")


def check_feature_materialization(payload, ctx):
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _tool_failed(drift):
        return _fault(False, "ai_infra")

    shift = drift.get("mean_shift_sigma")
    if shift is None:
        return _fault(False, "ai_infra")

    # The published clean baseline is a 3-sigma historical bound, while the
    # stream's clean values can reach about 0.472. The documented separation
    # point around 0.65 avoids turning ordinary serving noise into alerts.
    if shift > max(0.65, ctx.baseline["feature_mean_shift_sigma_max"] * 1.5):
        return _fault(True, "ai_infra", "training-serving feature drift")
    return _fault(False, "ai_infra")


def check_embedding_batch(payload, ctx):
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _tool_failed(drift):
        return _fault(False, "ai_infra")

    centroid_shift = drift.get("centroid_shift")
    avg_age = drift.get("avg_doc_age_days")
    if centroid_shift is None or avg_age is None:
        return _fault(False, "ai_infra")

    # Clean centroid drift is documented below 0.039 and clear faults start
    # above 0.040; corpus staleness faults begin around 48 days.
    if centroid_shift > 0.04:
        return _fault(True, "ai_infra", "embedding centroid drift")
    if avg_age >= 48.0:
        return _fault(True, "ai_infra", "embedding corpus is stale")
    return _fault(False, "ai_infra")
