library(testthat)
library(mockery)
library(withr)

test_that("s3saveRDS_HL envoie le bon chemin à s3saveRDS", {

  # --- Fake s3saveRDS pour capturer l'appel ---
  captured <- list()

  fake_s3save <- function(value, object, bucket, key, secret, region, base_url) {
    captured$object  <<- object
    captured$bucket  <<- bucket
    captured$key     <<- key
    captured$secret  <<- secret
    captured$region  <<- region
    captured$base_url <<- base_url
    TRUE
  }

  # stub dans s3saveRDS_HL
  stub(s3saveRDS_HL, "aws.s3::s3saveRDS", fake_s3save)

  # --- Variables d'environnement ---
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY     = "K1",
    HL_S3_SECRET  = "S1",
    HL_S3_BUCKET  = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION   = ""
  )


  # --- 1) Ajout automatique du main_folder ---
  captured <<- list()   # reset
  s3saveRDS_HL(value = 123, object_name = "fichier.rds")
  expect_equal(captured$object, "dossier/fichier.rds")


  # --- 2) Sans main_folder → chemin exact ---
  captured <<- list()
  s3saveRDS_HL(value = 123, object_name = "fichier.rds", main_folder = FALSE)
  expect_equal(captured$object, "fichier.rds")


  # --- 3) Sans main_folder + sous-dossier explicite ---
  captured <<- list()
  s3saveRDS_HL(value = 123, object_name = "dossier/fichier.rds", main_folder = FALSE)
  expect_equal(captured$object, "dossier/fichier.rds")

})
