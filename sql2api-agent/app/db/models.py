from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import json

Base = declarative_base()

class APIMapping(Base):
    __tablename__ = "api_mappings"

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    api_url = Column(String(255), nullable=False)
    method = Column(String(50), nullable=False)
    request_template = Column(String, nullable=True)

    def get_template_json(self):
        """将request_template字符串转换为JSON对象"""
        try:
            return json.loads(self.request_template) if self.request_template else {}
        except json.JSONDecodeError:
            return {}
