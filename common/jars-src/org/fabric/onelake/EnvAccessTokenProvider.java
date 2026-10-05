package org.fabric.onelake;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

/** Reads Azure CLI token from env (set by common.fabric_storage before Spark reads). */
public class EnvAccessTokenProvider implements CustomTokenProviderAdaptee {
  private String token = "";
  private long expiryEpochSec;

  @Override
  public void initialize(Configuration configuration, String accountName) throws IOException {
    loadToken();
  }

  @Override
  public String getAccessToken() throws IOException {
    loadToken();
    if (token == null || token.isEmpty()) {
      throw new IOException(
          "ONELAKE_ABFS_ACCESS_TOKEN is empty. Run az login, then re-run bootstrap.");
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
    String env = System.getenv("ONELAKE_ABFS_ACCESS_TOKEN");
    if (env != null && !env.trim().isEmpty()) {
      token = env.trim();
    }
    String file = System.getenv("ONELAKE_ABFS_TOKEN_FILE");
    if ((token == null || token.isEmpty()) && file != null && !file.trim().isEmpty()) {
      token = new String(Files.readAllBytes(Paths.get(file)), java.nio.charset.StandardCharsets.UTF_8)
          .trim();
    }
    String exp = System.getenv("ONELAKE_ABFS_TOKEN_EXPIRY");
    expiryEpochSec = 0;
    if (exp != null && !exp.trim().isEmpty()) {
      try {
        expiryEpochSec = Long.parseLong(exp.trim());
      } catch (NumberFormatException ignored) {
        expiryEpochSec = 0;
      }
    }
  }
}
