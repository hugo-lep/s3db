library(testthat)
library(mockery)
library(withr)

test_that("s3list_HL retourne correctement les listes de fichiers", {

  # --- Mock get_bucket() qui simule le comportement S3 ---
  fake_get_bucket <- function(bucket, prefix, key, secret, region, base_url, ...) {

    # 1) Le vrai chemin existant : "dossier_principal/sous-dossier"
    if (prefix == "dossier/sous-dossier") {
      return(list(
        Contents = list(
          list(Key = "dossier/sous-dossier/fichier1.rds"),
          list(Key = "dossier/sous-dossier/fichier2.rds")
        )
      ))
    }

    # 2) "sous-dossier" SANS main_folder -> aucun fichier
    if (prefix == "sous-dossier") {
      return(list())
    }

    # 3) Toute autre valeur -> aucun fichier
    return(list())
  }

  # stub dans s3list_HL
  stub(s3list_HL, "aws.s3::get_bucket", fake_get_bucket)

  # Variables d'environnement simulées
  local_envvar(
    HL_S3_MAIN_FOLDER = "dossier",
    HL_S3_KEY     = "K1",
    HL_S3_SECRET  = "S1",
    HL_S3_BUCKET  = "bucket",
    HL_S3_ENDPOINT = "http://endpoint",
    HL_S3_REGION   = ""
  )

  # --- 1) s3list_HL("sous-dossier") → ajoutera "dossier/" → doit retourner 2 fichiers
  res1 <- s3list_HL("sous-dossier")
  expect_true(is.list(res1))
  expect_true("Contents" %in% names(res1))
  expect_length(res1$Contents, 2)
  expect_equal(res1$Contents[[1]]$Key, "dossier/sous-dossier/fichier1.rds")
  expect_equal(res1$Contents[[2]]$Key, "dossier/sous-dossier/fichier2.rds")

  # --- 2) s3list_HL("sous-dossier", main_folder = FALSE) → sans préfixe -> vide
  res2 <- s3list_HL("sous-dossier", main_folder = FALSE)
  expect_identical(res2, list())

  # --- 3) s3list_HL(prefix = "dossier/sous-dossier", main_folder = FALSE) → retourne 2 fichiers
  res3 <- s3list_HL(prefix = "dossier/sous-dossier", main_folder = FALSE)
  # comme notre fake ne renvoie Contents pour portfolio/config_files, on vérifie qu'il est vide
  # si tu veux que ce cas retourne les deux fichiers, adapte le fake_get_bucket pour gérer ce prefix
  expect_true(is.list(res3))
  expect_true("Contents" %in% names(res3))
  expect_length(res3$Contents, 2)
  expect_equal(res3$Contents[[1]]$Key, "dossier/sous-dossier/fichier1.rds")
  expect_equal(res3$Contents[[2]]$Key, "dossier/sous-dossier/fichier2.rds")
})

