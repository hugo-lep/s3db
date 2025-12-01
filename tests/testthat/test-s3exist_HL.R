# tests/testthat/test-s3exist_HL.R

library(testthat)
library(mockery)
library(withr)

test_that("s3exist_HL retourne correctement TRUE/FALSE", {

  # --- Mock head_object qui simule le comportement S3 ---
  fake_head <- function(object, bucket, key, secret, region, base_url) {
    # seul le chemin exact "dossier/variable.rds" existe
    object == "dossier/variable.rds"
  }

  # Stub head_object dans s3exist_HL
  stub(s3exist_HL, "aws.s3::head_object", fake_head)

  # Variables d'environnement simulées
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY = "K1",
    HL_S3_SECRET = "S1",
    HL_S3_BUCKET = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION = ""
  )

  # --- Tests ---
  # 1) objet inexistant
  expect_false(s3exist_HL("test/config_files/users_auth.rds"))

  # 2) main_folder = FALSE, objet existant avec chemin complet
  expect_true(s3exist_HL("dossier/variable.rds", main_folder = FALSE))

  # 3) main_folder = FALSE, objet inexistant
  expect_false(s3exist_HL("variable.rds", main_folder = FALSE))

  # 4) main_folder = TRUE (préfixe "dossier/")
  expect_true(s3exist_HL("variable.rds"))
})
