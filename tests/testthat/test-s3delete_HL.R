library(testthat)
library(mockery)
library(withr)

test_that("s3delete_HL construit le bon chemin et retourne TRUE/FALSE", {

  # --- Fake delete_object pour capturer l'appel ---
  captured <- list()

  fake_delete <- function(object, bucket, key, secret, region, base_url) {
    captured$object  <<- object
    captured$bucket  <<- bucket
    captured$key     <<- key
    captured$secret  <<- secret
    captured$region  <<- region
    captured$base_url <<- base_url

    # True only for dossier/fichier.rds
    isTRUE(object == "dossier/fichier.rds")
  }

  # Stub
  stub(s3delete_HL, "aws.s3::delete_object", fake_delete)

  # --- ENV ---
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY     = "K1",
    HL_S3_SECRET  = "S1",
    HL_S3_BUCKET  = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION   = ""
  )


  # --- 1) Ajout automatique du main_folder ---
  captured <<- list()
  res1 <- s3delete_HL("fichier.rds")
  expect_equal(captured$object, "dossier/fichier.rds")
  expect_true(res1)


  # --- 2) Sans main_folder ---
  captured <<- list()
  res2 <- s3delete_HL("fichier.rds", main_folder = FALSE)
  expect_equal(captured$object, "fichier.rds")
  expect_false(res2)  # fake_delete retourne FALSE


  # --- 3) Sous-dossier explicite (sans main_folder) ---
  captured <<- list()
  res3 <- s3delete_HL("dossier/fichier.rds", main_folder = FALSE)
  expect_equal(captured$object, "dossier/fichier.rds")
  expect_true(res3)


  # --- 4) delete_object génère une erreur → FALSE ---
  stub(s3delete_HL, "aws.s3::delete_object", function(...) stop("boom"))
  res4 <- s3delete_HL("fichier.rds", main_folder = FALSE)
  expect_false(res4)

})
