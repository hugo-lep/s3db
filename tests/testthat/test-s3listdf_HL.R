library(testthat)
library(mockery)
library(withr)

test_that("s3listdf_HL retourne un dataframe correct", {

  # --- Mock s3list_HL ---
  fake_s3list_HL <- function(prefix, bucket, main_folder, key, secret, endpoint, region, max, ...) {
    list(
      list(Key = "dossier/sous-dossier/fichier1.rds", LastModified = "2025-12-02T12:00:00Z"),
      list(Key = "dossier/sous-dossier/fichier2.csv", LastModified = "2025-12-02T13:00:00Z"),
      list(Key = "dossier/sous-dossier/fichier3.rds", LastModified = "2025-12-02T14:00:00Z")
    )
  }

  # Stub dans s3listdf_HL
  stub(s3listdf_HL, "s3list_HL", fake_s3list_HL)

  # Variables d'environnement simulées
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY     = "K1",
    HL_S3_SECRET  = "S1",
    HL_S3_BUCKET  = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION   = ""
  )

  # --- Appel standard ---
  df <- s3listdf_HL("sous-dossier")

  expect_true(is.data.frame(df))
  expect_equal(ncol(df), 3)
  expect_named(df, c("fichier", "LastModified_UTC", "Key"))

  # Vérifie filtrage par défaut (.rds)
  expect_true(all(grepl("\\.rds$", df$fichier)))
  expect_equal(df$Key, c("fichier3", "fichier1")) # tri desc

  # Vérifie conversion LastModified en POSIXct
  expect_true(inherits(df$LastModified_UTC, "POSIXct"))

  # --- Cas type = NA (aucun filtrage) ---
  df2 <- s3listdf_HL("sous-dossier", type = NA)
  expect_equal(nrow(df2), 3)
})
