class Config:
    UPLOAD_FOLDER = "../data/uploads/"
    OUTPUT_FOLDER = "../data/output/"
    ALLOWED_EXTENSIONS = {"csv"}

    @staticmethod
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS

