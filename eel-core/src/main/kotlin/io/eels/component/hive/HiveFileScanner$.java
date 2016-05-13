object HiveFileScanner extends Logging {

  private val config = ConfigFactory.load()
  private val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  private val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  // returns true if the given file should be considered based on the config settings
  def skip(file: LocatedFileStatus): Boolean = {
    file.getLen == 0 || ignoreHiddenFiles && file.getPath.getName.matches(hiddenFilePattern)
  }

  def apply(location: String)(implicit fs: FileSystem): List[LocatedFileStatus] = {
    logger.debug(s"Scanning $location, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
    val path = new Path(location)
    if (fs.exists(path)) HdfsIterator(fs.listFiles(path, true)).filter(_.isFile).filterNot(skip).toList
    else Nil
  }
}
