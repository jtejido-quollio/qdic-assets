import logging
from logging.config import dictConfig
import os
import sys
from typing import List
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

ENV: str = ""


class Settings(BaseSettings):
    ENV: str = "dev"
    APP_NAME: str = "qdic-assets"
    SITE_NAME: str = "site-a"
    READINESS_PATH: str = "/healthz"
    CONTROLLER_URL: str = "http://qdic-controller.site-a.svc.cluster.local:8080"
    TENANT_ID: str | None = None
    ADVERTISE_ENDPOINT: str | None = None

    DATABASE_URL: str = "postgresql://postgres:postgres@db:5432/assets"

    RABBITMQ_URL: str = "amqp://guest:guest@rabbitmq:5672/"
    RABBITMQ_EVENTS_QUEUE: str = "app.public.events"

    # Worker pools
    EVENT_PROCESSOR_WORKER_POOL_SIZE: int = 1000
    EVENT_CONSUMER_WORKER_POOL_SIZE: int = 1

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


class TestSettings(Settings):
    ENV: str = "test"


settings = Settings()
if ENV == "prod":
    pass
elif ENV == "test":
    setting = TestSettings()
