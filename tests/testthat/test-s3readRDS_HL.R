library(testthat)
library(withr)
library(mockery)

test_that("Simulations avec bucket 'bucket' et fichier dans 'dossier/variable.rds'", {

  # on simule S3: au lieu de lire un fichier, on retourne ce qui est passé
  fake_s3 <- function(object, bucket, key, secret, region, base_url) {
    list(
      object = object,
      bucket = bucket,
      key = key,
      secret = secret,
      region = region,
      endpoint = base_url
    )
  }

  #dans la fonction s3readRDS_HL, la fonction aws.s3::s3readRDS, sera remplacé par fake_s3
  stub(s3readRDS_HL, "aws.s3::s3readRDS", fake_s3)

  # --- 1) object simple avec main_folder = TRUE ---
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY = "K1",
    HL_S3_SECRET = "S1",
    HL_S3_BUCKET = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION = ""
  )

  res1 <- s3readRDS_HL("variable.rds")
  expect_equal(res1$object, "dossier/variable.rds")
  expect_equal(res1$bucket, "bucket")

  # --- 2) main_folder = FALSE ---
  res2 <- s3readRDS_HL(
    object = "dossier/variable.rds",
    main_folder = FALSE
  )
  expect_equal(res2$object, "dossier/variable.rds")
  expect_equal(res2$bucket, "bucket")

  # --- 3) paramètres fournis explicitement ---
  res3 <- s3readRDS_HL(
    object = "dossier/variable.rds",
    bucket   = "bucket",
    main_folder = FALSE,
    key      = "K1",
    secret   = "S1",
    endpoint = "http://endpoint",
    region   = ""
  )

  expect_equal(res3$object, "dossier/variable.rds")
  expect_equal(res3$bucket, "bucket")

  # --- 4) paramètres fournis explicitement, main folder == "test" ---
  res4 <- s3readRDS_HL(
    object = "dossier/variable.rds",
    bucket   = "bucket",
    main_folder = "test",
    key      = "K1",
    secret   = "S1",
    endpoint = "http://endpoint",
    region   = ""
  )

  expect_equal(res4$object, "test/dossier/variable.rds")
  expect_equal(res4$bucket, "bucket")
})

