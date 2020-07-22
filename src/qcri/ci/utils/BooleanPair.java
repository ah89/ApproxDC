package qcri.ci.utils;

import java.util.Arrays;

public class BooleanPair {

  private boolean[] pair;

  public BooleanPair(boolean a, boolean b) {
    pair = new boolean[] {a,b};
  }

  public boolean[] getPair() {
    return pair;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BooleanPair that = (BooleanPair) o;

    return (that.getPair()[0] == this.getPair()[0] && that.getPair()[1] == this.getPair()[1]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(pair);
  }
}
