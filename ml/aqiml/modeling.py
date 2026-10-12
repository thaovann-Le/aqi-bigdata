"""Time-based split, XGBoost training, evaluation metrics and model persistence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import ALERT_AQI, SplitDates

XGB_PARAMS = {
    "n_estimators": 3000,
    "learning_rate": 0.03,
    "max_depth": 6,
    "min_child_weight": 5,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_lambda": 1.0,
    "tree_method": "hist",
    "early_stopping_rounds": 150,
    "eval_metric": "mae",
    "n_jobs": -1,
    "random_state": 42,
}


@dataclass(frozen=True)
class Split:
    train: pd.DataFrame
    valid: pd.DataFrame
    test: pd.DataFrame


def usable_rows(features: pd.DataFrame) -> pd.DataFrame:
    """Rows where both the target and the persistence baseline exist, so every model
    is scored on exactly the same hours."""
    return features.dropna(subset=["target", "persistence"])


# A row whose target is h hours after the validation window still sees validation-window AQI
# through its persistence (t = target - 24h) and lags (up to 24h before t): purge 24 + 24 hours.
PURGE_HOURS = 48


def time_split(features: pd.DataFrame, dates: SplitDates, purge_hours: int = PURGE_HOURS) -> Split:
    """Split on the *target* time so no training target lies inside validation or test.
    Validation sits inside the training era (polluted season), so training rows right after it
    are purged. Training still contains later months, so validation metrics are only used for
    early stopping; the test period (after all training targets) is the honest score."""
    t = features["target_ts"]
    in_valid = (t >= dates.valid_start) & (t < dates.valid_end)
    valid_end = pd.Timestamp(dates.valid_end)
    purged = (t >= valid_end) & (t < valid_end + pd.Timedelta(hours=purge_hours))
    return Split(
        train=features[(t >= dates.train_start) & (t < dates.test_start) & ~in_valid & ~purged],
        valid=features[in_valid],
        test=features[(t >= dates.test_start) & (t < dates.test_end)],
    )


def train_xgboost(split: Split, feature_cols: list[str], params: dict | None = None):
    """The model learns the 24h *change* (target - persistence): it only has to explain how
    tomorrow differs from now, instead of regressing the whole level towards the mean."""
    from xgboost import XGBRegressor  # imported lazily: notebooks don't need xgboost

    model = XGBRegressor(**(params or XGB_PARAMS))
    model.fit(
        split.train[feature_cols],
        split.train["target"] - split.train["persistence"],
        eval_set=[(split.valid[feature_cols], split.valid["target"] - split.valid["persistence"])],
        verbose=False,
    )
    return model


def predict_aqi(model, frame: pd.DataFrame, feature_cols: list[str]) -> np.ndarray:
    """Forecast = current AQI + predicted change, clipped to the AQI scale."""
    change = model.predict(frame[feature_cols])
    return np.clip(frame["persistence"].to_numpy() + change, 0, 500)


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    err = y_pred - y_true
    ss_tot = np.sum((y_true - y_true.mean()) ** 2)
    return {
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "bias": float(np.mean(err)),
        "r2": float(1 - np.sum(err**2) / ss_tot) if ss_tot > 0 else float("nan"),
    }


def alert_metrics(y_true: np.ndarray, y_pred: np.ndarray, threshold: int = ALERT_AQI) -> dict[str, float]:
    """Precision/recall of predicting AQI > threshold (the alert role C raises)."""
    actual = np.asarray(y_true) > threshold
    predicted = np.asarray(y_pred) > threshold
    tp = int(np.sum(actual & predicted))
    fp = int(np.sum(~actual & predicted))
    fn = int(np.sum(actual & ~predicted))
    precision = tp / (tp + fp) if tp + fp else float("nan")
    recall = tp / (tp + fn) if tp + fn else float("nan")
    f1 = 2 * precision * recall / (precision + recall) if tp else 0.0
    return {
        "alert_events": int(actual.sum()),
        "alert_precision": precision,
        "alert_recall": recall,
        "alert_f1": f1,
    }


def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {"n": int(len(y_true)), **regression_metrics(y_true, y_pred), **alert_metrics(y_true, y_pred)}


@dataclass(frozen=True)
class ModelMeta:
    city: str
    feature_set: str
    horizon_hours: int
    features: list[str]
    best_iteration: int
    split: dict[str, str]
    valid_metrics: dict[str, float]
    test_metrics: dict[str, float]
    baseline_test_metrics: dict[str, float]
    trained_at: str

    @property
    def model_name(self) -> str:
        return f"xgboost_{self.feature_set}_h{self.horizon_hours}"


def model_paths(model_dir: Path, city: str, feature_set: str, horizon: int) -> tuple[Path, Path]:
    stem = f"xgb_aqi_{city}_{feature_set}_h{horizon}"
    return model_dir / f"{stem}.json", model_dir / f"{stem}.meta.json"


def save_model(model, meta: ModelMeta, model_dir: Path) -> Path:
    model_path, meta_path = model_paths(model_dir, meta.city, meta.feature_set, meta.horizon_hours)
    model.save_model(model_path)
    meta_path.write_text(json.dumps(asdict(meta), indent=2, ensure_ascii=False), encoding="utf-8")
    return model_path


def load_model(model_dir: Path, city: str, feature_set: str, horizon: int):
    from xgboost import XGBRegressor

    model_path, meta_path = model_paths(model_dir, city, feature_set, horizon)
    if not model_path.exists():
        raise FileNotFoundError(f"{model_path} not found - run ml/training/train_xgboost.py first")
    model = XGBRegressor()
    model.load_model(model_path)
    meta = ModelMeta(**json.loads(meta_path.read_text(encoding="utf-8")))
    return model, meta
