package io.corps.sgoc.session.exceptions;

/**
 * Created by corps@github.com on 2014/03/06.
 * Copyrighted by Zach Collins 2014
 */
public class DependentPutReorderingRequest extends RuntimeException {
  private final String objectId;

  public DependentPutReorderingRequest(String objectId) {
    this.objectId = objectId;
  }

  public String getObjectId() {
    return objectId;
  }
}
