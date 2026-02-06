# =============================================================================
# ECF DataPulse Analytics - Dockerfile pour le pipeline ETL
# =============================================================================

FROM python:3.11-slim

# Métadonnées
LABEL maintainer="DataPulse Analytics"
LABEL description="Pipeline ETL pour la plateforme de données multi-sources"

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Répertoire de travail
WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copie des fichiers de dépendances
COPY requirements.txt .

# Installation des dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY src/ ./src/
COPY sql/ ./sql/

# Création des répertoires nécessaires
RUN mkdir -p /app/data /app/logs

# Utilisateur non-root pour la sécurité
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

# Point d'entrée par défaut
ENTRYPOINT ["python", "-m", "pipeline.etl_pipeline"]

# Arguments par défaut
CMD ["--help"]
