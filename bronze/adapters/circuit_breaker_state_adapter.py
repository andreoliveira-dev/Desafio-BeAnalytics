import os
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, sessionmaker
from bronze.ports.output_ports import CircuitBreakerStatePort

Base = declarative_base()


class CircuitBreakerStateModel(Base):
    __tablename__ = "circuit_breaker_states"

    name = sa.Column(sa.String(100), primary_key=True)
    state = sa.Column(sa.String(20), default="CLOSED")
    failure_count = sa.Column(sa.Integer, default=0)
    last_failure_time = sa.Column(sa.Float, default=0.0)


class InMemoryCircuitBreakerStateAdapter(CircuitBreakerStatePort):
    def __init__(self):
        self.db = {}

    def get_state(self, name: str) -> dict:
        if name not in self.db:
            return {"state": "CLOSED", "failure_count": 0, "last_failure_time": 0.0}
        return self.db[name]

    def update_state(self, name: str, state: str, failure_count: int, last_failure_time: float) -> None:
        self.db[name] = {
            "state": state,
            "failure_count": failure_count,
            "last_failure_time": last_failure_time
        }


class SqlCircuitBreakerStateAdapter(CircuitBreakerStatePort):
    def __init__(self, connection_string: str = None):
        if not connection_string:
            connection_string = os.getenv(
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
                "sqlite:///data/circuit_breaker.db"
            )

        if connection_string.startswith("sqlite:///"):
            db_path = connection_string.replace("sqlite:///", "")
            db_dir = os.path.dirname(db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

        self.engine = sa.create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def get_state(self, name: str) -> dict:
        with self.Session() as session:
            model = session.query(CircuitBreakerStateModel).filter_by(name=name).first()
            if not model:
                return {"state": "CLOSED", "failure_count": 0, "last_failure_time": 0.0}
            return {
                "state": model.state,
                "failure_count": model.failure_count,
                "last_failure_time": model.last_failure_time
            }

    def update_state(self, name: str, state: str, failure_count: int, last_failure_time: float) -> None:
        with self.Session() as session:
            model = session.query(CircuitBreakerStateModel).filter_by(name=name).first()
            if not model:
                model = CircuitBreakerStateModel(name=name)
                session.add(model)
            model.state = state
            model.failure_count = failure_count
            model.last_failure_time = last_failure_time
            session.commit()
