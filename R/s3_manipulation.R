
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

  if (!(is.character(main_folder) || isFALSE(main_folder))) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # si main_folder n'est pas NA, on l'ajoute devant l'objet
  if (!isFALSE(main_folder)) {
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

  object2 <- object
  # Préfixer si demandé
  if (!isFALSE(main_folder)) {
    object2 <- paste0(main_folder, "/", object)
  }

  # head_object silencieux et sûr
  exists <- tryCatch(
#    suppressMessages(
#      suppressWarnings(
        aws.s3::head_object(
          object = object2,
#          object = "test",
          bucket = bucket,
          key = key,
          secret = secret,
          region = region,
          base_url = endpoint
#        )
#      )
    ),
    error = function(e) NULL  # si erreur R → NULL
  )

  isTRUE(exists)

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
#' @param max maximum de fichiers retourner, max = INF pour tout avoir
#' @param ... Autres paramètres transmis à une fonction interne ou à une méthode
#'
#' @return Liste d'objets (comme aws.s3::get_bucket)
#' @export
s3list_HL <- function(prefix = "",
                      bucket = NA,
                      main_folder = TRUE,
                      key = NA,
                      secret = NA,
                      endpoint = NA,
                      region = NA,
                      max = NULL,
                      ...) {

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
  if (!(is.character(main_folder) || isFALSE(main_folder))) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # Ajouter dossier principal si demandé
  full_prefix <- prefix
  if (!isFALSE(main_folder) && nzchar(main_folder)) {
    full_prefix <- paste0(main_folder, "/", prefix)
    # Normalisation : pas de double-slash
    full_prefix <- gsub("//+", "/", full_prefix)
  }

  # Appel aws.s3
  aws.s3::get_bucket(
    bucket = bucket,
    prefix = full_prefix,
    key = key,
    secret = secret,
    region = region,
    base_url = endpoint,
    max = max
  )
}

utils::globalVariables(c(
  "Key","fichier","LastModified_UTC"
))
#' @title Liste de fichier
#'
#' @description
#' Retourne la liste des fichiers d'un bucket et dossier précis de S3
#'
#' @param prefix Sous-dossier (peut être "")
#' @param bucket Nom du bucket
#' @param main_folder Si TRUE, préfixe automatiquement "HL_S3_MAIN_FOLDER"
#' @param key S3 key
#' @param secret S3 secret
#' @param endpoint Base URL S3
#' @param region Region (AWS requis, OVH="" )
#' @param max maximum de fichiers retourner, max = INF pour tout avoir
#' @param type Pour ne filtrer qu'un extension de fichier
#' @param lastModified "lubridate"
#' @param ... Autres paramètres transmis à une fonction interne ou à une méthode
#'
#' @importFrom stringr str_detect str_remove str_c fixed
#' @importFrom dplyr mutate arrange rename desc filter
#' @importFrom magrittr %>%
#' @importFrom lubridate ymd_hms
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @importFrom tools  file_path_sans_ext
#'
#' @returns un dataframe avec les informations du dossier demandé
#' @export
#'
#' @examples
#' if (interactive()){
#' files_all <- s3listdf_HL("OTP/legs_history")
#' }
s3listdf_HL <- function(prefix = "",
                        bucket = NA,
                        main_folder = TRUE,
                        key = NA,
                        secret = NA,
                        endpoint = NA,
                        region = NA,
                        max = NULL,
                        type = ".rds",
                        lastModified = "lubridate",
                        ...) {

  df <- s3list_HL(prefix = prefix,
            bucket = bucket,
            main_folder = main_folder,
            key = key,
            secret = secret,
            endpoint = endpoint,
            region = region,
            max = max) %>%

    purrr::map_dfr(~ tibble::tibble(
      fichier = .x[["Key"]],
      LastModified_UTC = .x[["LastModified"]]
    ))

    # Filtrer selon le type si type n'est pas NA
    if (!is.na(type)) {
      df <- df %>% dplyr::filter(stringr::str_detect(fichier, fixed(type)))
    }

    # Créer Key
    df <- df %>% dplyr::mutate(
      Key = tools::file_path_sans_ext(basename(fichier))) %>%
      dplyr::arrange(dplyr::desc(Key))

        # Modifier LastModified si demandé
    if (!is.na(lastModified) && lastModified == "lubridate") {
      df <- df %>% dplyr::mutate(LastModified_UTC = lubridate::ymd_hms(LastModified_UTC, tz = "UTC"))
    }
    return(df)
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
  if (!(is.character(main_folder) || isFALSE(main_folder))) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # si main_folder n'est pas NA, on l'ajoute devant l'objet
  if (!isFALSE(main_folder)) {
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

#' Supprimer un fichier sur S3 (OVH ou AWS) avec aws.s3
#'
#' @param object (chr) Fichier à supprimer (avec sous-dossier)
#' @param bucket (chr) Bucket S3
#' @param main_folder (log|chr) TRUE = utilise HL_S3_MAIN_FOLDER, sinon dossier ou ""
#' @param key (chr) S3 key
#' @param secret (chr) S3 secret
#' @param endpoint (chr) Endpoint S3
#' @param region (chr) Région
#'
#' @returns TRUE si suppression OK, FALSE sinon
#' @export
s3delete_HL <- function(object,
                        bucket = NA,
                        main_folder = TRUE,
                        key = NA,
                        secret = NA,
                        endpoint = NA,
                        region = NA) {

  # --- Charger infos depuis variables d'env ---
  if (is.na(key))       key       <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret))    secret    <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket))    bucket    <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region))   region   <- Sys.getenv("HL_S3_REGION")

  # --- Vérifications ---
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement (key/secret/bucket/endpoint/region).")
  }

  if (!(is.character(main_folder) || isFALSE(main_folder))) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # si main_folder n'est pas NA, on l'ajoute devant l'objet
  if (!isFALSE(main_folder)) {
    object <- paste0(main_folder, "/", object)
  }

  # --- Suppression ---
  tryCatch({
    res <- aws.s3::delete_object(
      object = object,
      bucket = bucket,
      key = key,
      secret = secret,
      region = region,
      base_url = endpoint
    )

    # delete_object() retourne TRUE si succès
    isTRUE(res)

  }, error = function(e) {
    FALSE
  })
}


