from aau_ais_core.settings import GizmoSqlConnectionSettings
from aau_ais_core.settings import Settings as CoreSettings
from pydantic import BaseModel, Field


class TrajectorySettings(BaseModel):
    min_speed_threshold: float = 1  # In knots
    max_speed_threshold: float = 100  # In knots
    temporal_threshold: int = Field(default=600, gt=180)
    spatial_threshold: int = 4000  # In meters
    start_trajectory_window_size: int = 3
    stop_trajectory_window_size: int = 20


class Settings(CoreSettings):
    gizmosql: GizmoSqlConnectionSettings
    trajectory: TrajectorySettings = TrajectorySettings()
