#' Lire un fichier .rds depuis S3 avec paws
#'
#' @param object Fichier à lire (avec sous dossier)
#' @param bucket Bucket S3
#' @param main_folder Dossier à ajouter automatiquement devant l'objet
#' @param key Key pour accès au S3
#' @param secret Secret pour accès au S3
#' @param endpoint Endpoint pour accéder au S3
#' @param region Region (AWS requis, OVH peut être "")
#'
#' @returns Valeur du fichier .rds
#' @export
#'
#' @examples
#' if(interactive()){
#'   s3readRDS_HL_paws("mon_fichier.rds")
#' }
s3readRDS_HL_paws <- function(object,
                              bucket = NA,
                              main_folder = NA,
                              key = NA,
                              secret = NA,
                              endpoint = NA,
                              region = NA) {

  # Valeurs par défaut depuis l'environnement
  if (is.na(key))        key        <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret))     secret     <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket))     bucket     <- Sys.getenv("HL_S3_BUCKET")
  if (is.na(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint))   endpoint   <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region))     region     <- Sys.getenv("HL_S3_REGION")

  # Vérification minimale
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }

  # Ajouter main_folder si présent
  if (!is.na(main_folder)) {
    object <- paste0(main_folder, "/", object)
  }

  # Initialiser le client S3 avec paws
  s3 <- paws::s3(
    config = list(
      credentials = list(
        creds = list(
          access_key_id     = key,
          secret_access_key = secret
        )
      ),
      region = region,
      endpoint = endpoint
    )
  )

  # Télécharger le fichier sous forme de raw vector
  raw_obj <- s3$get_object(Bucket = bucket, Key = object)$Body

  # Lire le RDS depuis la connexion raw
  readRDS(rawConnection(raw_obj))
}
