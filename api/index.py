"""Vercel function entry point.

The application stays in backend/app so local development and deployment use
the same FastAPI routes.
"""

from backend.app.main import app
