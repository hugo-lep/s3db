
#' Fonction pour lire un .rds sur S3
#'
#' @param object Fichier à lire (avec sous dossier)
#' @param bucket Bucket S3
#' @param main_folder (log) Dossier principal dans "HL_S3_MAIN_FOLDER" ajouté devant object à lire
#' @param key Key pour accès au S3
#' @param secret Secret pour accès au S3
#' @param endpoint Enpoint pour accéder au S3
#' @param region Region (parfoit nécessaire), AWS est requis, OVH doit == ""
#'
#' @importFrom aws.s3 s3readRDS
#'
#' @returns Valeur du fichier .rds
#' @export
#'
#' @examples
#' if(interactive()){
#' s3readRDS_HL(object)
#' }
s3readRDS_HL <- function(object,
                         bucket = NA,
                         main_folder = TRUE,
                         key = NA,
                         secret = NA,
                         endpoint = NA,
                         region = NA) {



  #  Récupérer la valeur depuis la variable environnement si argument = NA
  if (is.na(key)) key <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret)) secret <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket)) bucket <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region)) region <- Sys.getenv("HL_S3_REGION")

  #  Vérification minimale
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }
  if (!is.character(main_folder)) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # si main_folder n'est pas NA, on l'ajoute devant l'objet
  if (!is.na(main_folder)) {
    object <- paste0(main_folder, "/", object)
  }

  #  Appel à aws.s3
  aws.s3::s3readRDS(
    object = object,
    bucket = bucket,
    key = key,
    secret = secret,
    region = region,
    base_url = endpoint
  )
}

#' Vérifier si un objet existe sur S3 (aws.s3, wrapper HL)
#'
#' @param object Chemin de l'objet (sans main_folder)
#' @param bucket Bucket S3
#' @param main_folder (log) TRUE pour préfixer object par HL_S3_MAIN_FOLDER
#' @param key Key pour accès au S3
#' @param secret Secret pour accès au S3
#' @param endpoint Endpoint (base_url) pour accéder au S3 (ex: "s3.bhs.cloud.ovh.net")
#' @param region Region (parfois nécessaire). Pour OVH, peut être "".
#'
#' @returns TRUE si l'objet existe, FALSE sinon
#' @export
s3exist_HL <- function(object,
                        bucket = NA,
                        main_folder = TRUE,
                        key = NA,
                        secret = NA,
                        endpoint = NA,
                        region = NA) {

  # Récupérer les valeurs via env vars si manquantes
  if (is.na(key))     key     <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret))  secret  <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket))  bucket  <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region))   region   <- Sys.getenv("HL_S3_REGION")

  # Vérifications
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }
  if (isTRUE(main_folder) && !is.character(main_folder)) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être présente dans 'HL_S3_MAIN_FOLDER'.")
  }

  # Préfixer si demandé
  if (!is.na(main_folder)) {
    object <- paste0(main_folder, "/", object)
  }

  # head_object via aws.s3 : renvoie méta si existe, lève erreur si 404
  exists <- tryCatch({
    aws.s3::head_object(
      object = object,
      bucket = bucket,
      key = key,
      secret = secret,
      region = region,
      base_url = endpoint
    )
    TRUE
  }, error = function(e) {
    # Si erreur (404 ou autre), on retourne FALSE.
    # On pourrait inspecter e$message pour distinguer les erreurs si besoin.
    FALSE
  })

  return(isTRUE(exists))
}

#' Liste les objets dans un bucket ou sous-dossier S3
#'
#' @param prefix Sous-dossier (peut être "")
#' @param bucket Nom du bucket
#' @param main_folder Si TRUE, préfixe automatiquement "HL_S3_MAIN_FOLDER"
#' @param key S3 key
#' @param secret S3 secret
#' @param endpoint Base URL S3
#' @param region Region (AWS requis, OVH="" )
#'
#' @return Liste d'objets (comme aws.s3::get_bucket)
#' @export
s3list_HL <- function(prefix = "",
                      bucket = NA,
                      main_folder = TRUE,
                      key = NA,
                      secret = NA,
                      endpoint = NA,
                      region = NA) {

  # Lire valeurs depuis variables d'environnement
  if (is.na(key)) key <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret)) secret <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket)) bucket <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region)) region <- Sys.getenv("HL_S3_REGION")

  # Vérifications
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }
  if (!is.character(main_folder)) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrite dans HL_S3_MAIN_FOLDER.")
  }

  # Ajouter dossier principal si demandé
  full_prefix <- prefix
  if (!is.na(main_folder) && nzchar(main_folder)) {
    full_prefix <- paste0(main_folder, "/", prefix)
  }

  # Normalisation : pas de double-slash
  full_prefix <- gsub("//+", "/", full_prefix)

  # Appel aws.s3
  aws.s3::get_bucket(
    bucket = bucket,
    prefix = full_prefix,
    key = key,
    secret = secret,
    region = region,
    base_url = endpoint
  )
}

#' Fonction pour sauvegarder un .rds sur S3 (OVH compatible)
#'
#' @param object_name Nom du fichier à créer dans S3 (peut inclure sous-dossiers)
#' @param value Objet R à sauvegarder
#' @param bucket Bucket S3
#' @param main_folder (logique) Ajouter automatiquement HL_S3_MAIN_FOLDER ?
#' @param key S3 key
#' @param secret S3 secret
#' @param endpoint Endpoint OVH
#' @param region Région ("" pour OVH)
#'
#' @return TRUE si succès, erreur sinon
#' @export
s3saveRDS_HL <- function(value,
                         object_name,
                         bucket = NA,
                         main_folder = TRUE,
                         key = NA,
                         secret = NA,
                         endpoint = NA,
                         region = NA) {

  #--- Récupération depuis variables d'environnement si manquants
  if (is.na(key))      key      <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret))   secret   <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket))   bucket   <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region))   region   <- Sys.getenv("HL_S3_REGION")

  #--- Vérifications
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement.")
  }
  if (!is.character(main_folder)) {
    stop("Pour utiliser 'main_folder = TRUE', HL_S3_MAIN_FOLDER doit être défini.")
  }

  #--- Construction du chemin final
  if (!is.na(main_folder)) {
    object_name <- paste0(main_folder, "/", object_name)
  }


  aws.s3::s3saveRDS(
    value,
    object = object_name,
    bucket = bucket,
    key = key,
    secret = secret,
    region = region,
    base_url = endpoint
  )
}

