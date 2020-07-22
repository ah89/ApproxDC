package qcri.ci.utils;

import java.util.Arrays;

public class IntegerPair {

  private int[] pair;

  public IntegerPair(int a, int b) {
    pair = new int[] {a,b};
  }

  public int[] getPair() {
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

    IntegerPair that = (IntegerPair) o;

    return (that.getPair()[0] == this.getPair()[0] && that.getPair()[1] == this.getPair()[1]);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(pair);
  }
}
