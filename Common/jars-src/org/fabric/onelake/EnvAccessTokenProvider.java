package org.fabric.onelake;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

/** Azure CLI token for abfss (Python sets Hadoop conf + ~/.fabric/onelake_abfs_token). */
public class EnvAccessTokenProvider implements CustomTokenProviderAdaptee {
  static final String CONF_ACCESS_TOKEN = "org.fabric.onelake.access.token";
  static final String CONF_TOKEN_FILE = "org.fabric.onelake.token.file";
  static final String CONF_TOKEN_EXPIRY = "org.fabric.onelake.token.expiry";

  private Configuration hadoopConf;
  private String token = "";
  private long expiryEpochSec;

  @Override
  public void initialize(Configuration configuration, String accountName) throws IOException {
    this.hadoopConf = configuration;
    loadToken();
  }

  @Override
  public String getAccessToken() throws IOException {
    loadToken();
    if (token == null || token.isEmpty()) {
      throw new IOException(
          "OneLake ABFS token missing. Run az login, re-run bootstrap, then read_table(). "
              + "Expected token file: "
              + defaultTokenCachePath());
    }
    return token;
  }

  @Override
  public Date getExpiryTime() {
    if (expiryEpochSec <= 0) {
      return new Date(System.currentTimeMillis() + 3600_000L);
    }
    return new Date(expiryEpochSec * 1000L);
  }

  private void loadToken() throws IOException {
    token = "";
    expiryEpochSec = 0;

    if (hadoopConf != null) {
      token = trimOrEmpty(hadoopConf.get(CONF_ACCESS_TOKEN));
      expiryEpochSec = parseExpiry(hadoopConf.get(CONF_TOKEN_EXPIRY));
      if (token.isEmpty()) {
        token = readTokenFile(trimOrEmpty(hadoopConf.get(CONF_TOKEN_FILE)));
      }
    }

    if (token.isEmpty()) {
      token = readTokenFile(defaultTokenCachePath());
    }

    if (token.isEmpty()) {
      token = trimOrEmpty(System.getenv("ONELAKE_ABFS_ACCESS_TOKEN"));
    }
    if (token.isEmpty()) {
      token = readTokenFile(trimOrEmpty(System.getenv("ONELAKE_ABFS_TOKEN_FILE")));
    }

    if (expiryEpochSec <= 0) {
      expiryEpochSec = parseExpiry(System.getenv("ONELAKE_ABFS_TOKEN_EXPIRY"));
    }
  }

  private static String defaultTokenCachePath() {
    return Paths.get(System.getProperty("user.home"), ".fabric", "onelake_abfs_token")
        .toString();
  }

  private static String trimOrEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static long parseExpiry(String value) {
    if (value == null || value.trim().isEmpty()) {
      return 0;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException ignored) {
      return 0;
    }
  }

  private static String readTokenFile(String path) throws IOException {
    if (path == null || path.isEmpty()) {
      return "";
    }
    java.nio.file.Path p = Paths.get(path);
    if (!Files.isRegularFile(p)) {
      return "";
    }
    return new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8).trim();
  }
}
