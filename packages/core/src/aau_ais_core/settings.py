from functools import cache
from urllib import parse

from pydantic import BaseModel, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GizmoSqlConnectionSettings(BaseModel):
    host: str
    port: int = 3306
    user: str
    password: SecretStr
    use_tls: bool = False
    extra_db_params: dict | None = None
    conn_kwargs: dict | None = None

    @property
    def uri(self):
        scheme = "grpc+tls" if self.use_tls else "grpc"
        return f"{scheme}://{self.host}:{self.port}"

    @property
    def db_kwargs(self):
        params = self.extra_db_params or {}
        credentials = {
            "username": self.user,
            "password": self.password.get_secret_value(),
        }
        return {**credentials, **params}


class DataWarehouseConnectionSettings(BaseModel):
    host: str
    port: int
    name: str
    user: str
    password: SecretStr
    application_name: str | None = None

    @computed_field
    @property
    def conn_str(self) -> str:
        con_str = f"postgresql://{self.user}:{parse.quote(self.password.get_secret_value())}@{self.host}:{self.port}/{self.name}"

        if self.application_name is not None:
            con_str += f"?application_name={self.application_name}"
        return con_str


class Settings(BaseSettings):
    # files in '/run/secrets' take priority over '/var/run'
    model_config = SettingsConfigDict(
        secrets_dir=("/var/run", "/run/secrets"),
        extra="ignore",
        env_file=".env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )

    @classmethod
    @cache
    def create(cls):
        return cls()  # type: ignore
