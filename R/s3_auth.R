#S3_ACCESS_KEY_ID_HL
#S3_SECRET_ACCESS_KEY_HL
#S3_BUCKET_NAME_HL
#S3_DEFAULT_REGION_HL
#S3_ENDPOINT_HL
#' S3 initialisation
#'
#' @param key (chr) S3 key
#' @param secret (chr) s3 secret
#' @param main_folder (chr) folder added behind object
#' @param bucket (chr) s3 bucket
#' @param endpoint (chr) endpoint nécessaire pour accéder à S3
#' @param region (chr) s3 region, OVH == ""
#' @param use_config_files (log) TRUE if use with files config files
#'
#' @importFrom aws.s3 s3readRDS
#' @importFrom sodium hash data_decrypt
#'
#' @returns Rien, mais crée des variables environnement simplifiant la connexion à S3
#' @export
#'
#' @examples
#' if(interactive()){
#' s3_connection_HL()
#' }
s3_connection_HL <- function(key = NA,
                             secret = NA,
                             main_folder = NA,
                             bucket = NA,
                             endpoint = NA,
                             region = NA,
                             use_config_files = FALSE) {

  #  Si use_aws_config_file = TRUE, on lit les anciens fichiers AWS chiffrés

  if (use_config_files) {
    config_s3_access <- readRDS("inst/app/data/config_s3_access.rds")
    config_s3_location <- readRDS("inst/app/data/config_s3_location.rds")
    secure_key_raw <- hash(charToRaw(config_s3_location$secure_key))

    if (is.na(key))    key      <- rawToChar(data_decrypt(bin = config_s3_access$S3_ACCESS_KEY_ID_HL, key = secure_key_raw))
    if (is.na(secret)) secret   <- rawToChar(data_decrypt(bin = config_s3_access$S3_SECRET_ACCESS_KEY_HL, key = secure_key_raw))
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
    if (is.na(main_folder)) main_folder   <- config_s3_location$s3_main_folder
    if (is.na(region)) region   <- config_s3_access$S3_region
    if (is.na(endpoint)) endpoint <- config_s3_access$S3_endpoint
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
  }

  #  Mettre en place les variables environnement HL
  Sys.setenv(
    HL_S3_KEY = key,
    HL_S3_SECRET = secret,
    HL_S3_MAIN_FOLDER = main_folder,
    HL_S3_BUCKET = bucket,
    HL_S3_ENDPOINT = endpoint,
    HL_S3_REGION = region
  )

  message("Connexion S3 initialisée avec s3_connection_HL")
}



# ---- 2e version paws ----------------------------------------------------

#' @title s3 create client
#'
#' @param key (chr) S3 key
#' @param secret (chr) s3 secret
#' @param main_folder (chr) folder added behind object
#' @param bucket (chr) s3 bucket
#' @param endpoint (chr) endpoint nécessaire pour accéder à S3
#' @param region (chr) s3 region, OVH == ""
#' @param use_config_files (log) TRUE if use with files config files
#'
#' @importFrom paws s3
#' @importFrom sodium hash
#'
#' @returns paws variable
#' @export
#'
#' @examples
#' if(interactive()){
#' s3_client_setup()
#' }
s3_client_setup <- function(key = NA,
                            secret = NA,
                            main_folder = NA,
                            bucket = NA,
                            endpoint = NA,
                            region = NA,
                            use_config_files = FALSE) {

  #  Lecture des fichiers de config si demandé

  if (use_config_files) {
    config_s3_access <- readRDS("inst/app/data/config_s3_access.rds")
    config_s3_location <- readRDS("inst/app/data/config_s3_location.rds")
    secure_key_raw <- sodium::hash(charToRaw(config_s3_location$secure_key))

    if (is.na(key))    key      <- rawToChar(sodium::data_decrypt(bin = config_s3_access$S3_ACCESS_KEY_ID_HL, key = secure_key_raw))
    if (is.na(secret)) secret   <- rawToChar(sodium::data_decrypt(bin = config_s3_access$S3_SECRET_ACCESS_KEY_HL, key = secure_key_raw))
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
    if (is.na(main_folder)) main_folder <- config_s3_location$s3_main_folder
    if (is.na(region)) region   <- config_s3_access$S3_region
    if (is.na(endpoint)) endpoint <- config_s3_access$S3_endpoint

  }

  #  Vérification minimale
  if (any(is.na(c(key, secret, bucket, region, endpoint)))) {
    stop("S3 non initialisé correctement. Vérifiez vos fichiers de config ou les arguments passés.")
  }

  #  Création du client paws
  client <- paws::s3(
    credentials = list(
      creds = list(
        access_key_id = key,
        secret_access_key = secret
      )
    ),
    endpoint = endpoint,
    region = region
  )

  #  Ajouter les infos supplémentaires dans l'objet client
  client$main_folder <- main_folder

  message("Connexion S3 initialisée avec paws et informations stockées dans le client")
  return(client)
}
