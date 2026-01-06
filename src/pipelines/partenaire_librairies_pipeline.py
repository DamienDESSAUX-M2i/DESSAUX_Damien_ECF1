import hashlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.extractors import APIAdressExtractor, ExcelExtractor
from src.storage import MinIOStorage, PostgreSQLStorage

logger = logging.getLogger("app")


class PartenaireLibrairiesPipeline:
    def __init__(self, dir_path: Path):
        self.dir_path = dir_path
        self.excel_extractor = ExcelExtractor()
        self.api_adress_extractor = APIAdressExtractor()
        self.postgresql_storage = PostgreSQLStorage()
        self.minio_storage = MinIOStorage()

    def run(self):
        try:
            logger.info("=" * 3)
            logger.info("PARTENAIRE LIBRAIRIES PIPELINE STARTED")
            logger.info("=" * 3)

            logger.info("[1/3] EXTRACTION")
            dict_dfs = self._extract()

            logger.info("[2/3] TRANSFORMATION")
            dict_dfs_transformed = self._transform(dict_dfs)

            logger.info("[3/3] LOADING")
            self._load(dict_dfs_transformed)

            logger.info("=" * 3)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 3)

        except Exception as e:
            logger.error(f"PIPELINE ERROR : {e}")

    def _extract(self) -> dict[str, pd.DataFrame]:
        excel_file_path = self.dir_path / "data" / "partenaire_librairies.xlsx"
        dict_dfs = self.excel_extractor.extract(file_path=excel_file_path)

        df = dict_dfs["Librairies Partenaires"]

        data_geo = []
        for _, row in df.iterrows():
            params = {"q": row["adresse"], "postcode": row["code_postal"], "limit": 1}
            data = self.api_adress_extractor.get(params=params)
            try:
                lat = data["features"][0]["geometry"]["coordinates"][0]
                lon = data["features"][0]["geometry"]["coordinates"][1]
            except Exception:
                lat = None
                lon = None
            data_geo.append({"latitude": lat, "longitude": lon})
            # politesse
            time.sleep(1)

        df_geo = pd.DataFrame(data_geo)

        dict_dfs["Librairies Partenaires"] = pd.concat([df, df_geo], axis=1)

        return dict_dfs

    def _transform(self, dict_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        df = dict_dfs["Librairies Partenaires"]

        # Drop Duplicates
        logger.info("Drop duplicates")
        df.drop_duplicates()

        # type
        logger.info("Force type")
        df["code_postal"] = df["code_postal"].astype("str")
        df["contact_telephone"] = df["contact_telephone"].astype("str")

        # Fill NaN
        logger.info("Fill NaN")
        df["nom_librairie"] = df["nom_librairie"].fillna(value="Unknown")
        df["adresse"] = df["adresse"].fillna(value="Unknown")
        df["code_postal"] = df["code_postal"].fillna(value="00000")
        df["ville"] = df["ville"].fillna(value="Unknown")
        df["contact_nom"] = df["contact_nom"].fillna(value="Unknown")
        df["contact_email"] = df["contact_email"].fillna(value="Unknown")
        df["contact_telephone"] = df["contact_telephone"].fillna(value="0000000000")
        df["ca_annuel"] = df["ca_annuel"].fillna(value=0.0)
        df["date_partenariat"] = df["date_partenariat"].fillna(
            value=datetime.now(timezone.utc)
        )
        df["specialite"] = df["specialite"].fillna(value="Unknown")
        df["latitude"] = df["latitude"].fillna(value=0)
        df["longitude"] = df["longitude"].fillna(value=0)

        # RGPD
        logger.info("Apply RGPD restriction")
        df["contact_initiales"] = df["contact_nom"].apply(
            lambda x: "".join([word[0] for word in x.split()])
        )
        df.drop(columns=["contact_nom"], inplace=True)
        df["contact_email"] = df["contact_email"].apply(
            lambda x: hashlib.md5(str(x).encode()).hexdigest()
        )
        df["contact_telephone"] = df["contact_telephone"].apply(
            lambda x: hashlib.md5(str(x).encode()).hexdigest()
        )

        dict_dfs["Librairies Partenaires"] = df

        return dict_dfs

    def _load(self, dict_dfs_transformed: dict[str, pd.DataFrame]) -> None:
        df = dict_dfs_transformed["Librairies Partenaires"]

        # Export JSON
        self.minio_storage.upload_json(
            data=df.to_json(index=False),
            filename=f"librairies_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json",
        )

        nb_librairies = 0
        nb_ca_annuel = 0
        for index, row in df.iterrows():
            result = self.postgresql_storage.insert_into_librairies(
                nom_librairie=row["nom_librairie"],
                adresse=row["adresse"],
                code_postal=row["code_postal"],
                ville=row["ville"],
                contact_initiales=row["contact_initiales"],
                contact_email=row["contact_email"],
                contact_telephone=row["contact_telephone"],
                date_partenariat=row["date_partenariat"],
                specialite=row["specialite"],
                latitude=row["latitude"],
                longitude=row["longitude"],
            )

            if result:
                nb_librairies += 1

                id_librairie = result["id_librairie"]
                result = self.postgresql_storage.insert_into_ca_annuel(
                    ca_annuel=row["ca_annuel"], id_librairie=id_librairie
                )

                if result:
                    nb_ca_annuel += 1

        logger.info(f"Number of librairies inserted: {nb_librairies}")
        logger.info(f"Number of ca_annuel inserted: {nb_ca_annuel}")

        self.postgresql_storage.close()
