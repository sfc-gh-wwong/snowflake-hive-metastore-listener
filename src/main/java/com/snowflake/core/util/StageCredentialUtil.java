/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import org.apache.hadoop.conf.Configuration;

import java.lang.UnsupportedOperationException;

/**
 * A utility function to help with retrieving stage credentials and generating
 * the credential string for Snowflake.
 */
public class StageCredentialUtil
{
  // TODO: Support more stages
  public enum StageCredentialType
  {
    AWS;
  }

  /**
   * Get a stage type from the Hive table location
   * @param url The URL
   * @return Snowflake's corresponding stage type
   * @throws IllegalArgumentException Thrown when the URL or stage type is
   *                                  invalid or unsupported.
   */
  private static StageCredentialType getStageTypeFromURL(String url)
    throws IllegalArgumentException
  {
    if (url.startsWith("s3"))
    {
      return StageCredentialType.AWS;
    }
    throw new IllegalArgumentException(
        "The stage type does not exist or is unsupported for URL: " + url);
  }

  /**
   * Get the prefix of the location from the hiveUrl
   * Used for retrieving keys from the config.
   * @param hiveUrl The URL
   * @return The prefix/protocol from the URL, e.g. s3, s3a
   */
  private static String getLocationPrefix(String hiveUrl)
  {
    int colonIndex = hiveUrl.indexOf(":");
    return hiveUrl.substring(0, colonIndex);
  }

  /**
   * Get the credentials for the given stage in the url, for example:
   *  credentials=(AWS_KEY_ID='{accessKey}' AWS_SECRET_KEY='{awsSecretKey}')
   * @param url The URL
   * @param config The Hadoop configuration
   * @return Snippet that represents credentials for the given location
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public static String generateCredentialsString(
      String url, Configuration config)
  {
    try
    {
      StageCredentialType stageType = getStageTypeFromURL(url);

      switch(stageType)
      {
        case AWS:
          // Get the AWS access keys
          // Note: with s3a, "fs.s3a.access.key" is also a valid configuration,
          //       so check both "fs.X.access.key" and "fs.X.awsAccessKeyId"
          String prefix = getLocationPrefix(url);
          String accessKey = config.get("fs." + prefix + ".awsAccessKeyId");
          if (accessKey == null)
          {
            accessKey = config.get("fs." + prefix + ".access.key");
          }

          String secretKey = config.get("fs." + prefix + ".awsSecretAccessKey");
          if (secretKey == null)
          {
            secretKey = config.get("fs." + prefix + ".secret.key");
          }

          return String.format(
              "credentials=(AWS_KEY_ID='%s'\nAWS_SECRET_KEY='%s')",
              StringUtil.escapeSqlText(accessKey),
              StringUtil.escapeSqlText(secretKey));

        default:
          throw new UnsupportedOperationException("Unsupported stage type: " +
                                                  stageType.name());
      }
    }
    catch (Exception e)
    {
      return String.format(
          "credentials=(/* Error generating credentials expression: %s */)",
          e.getMessage());
    }
  }
}
