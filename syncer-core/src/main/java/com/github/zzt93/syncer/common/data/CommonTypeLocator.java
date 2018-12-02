package com.github.zzt93.syncer.common.data;

import org.springframework.expression.EvaluationException;
import org.springframework.expression.TypeLocator;
import org.springframework.expression.spel.support.StandardTypeLocator;

/**
 * @author zzt
 */
public class CommonTypeLocator implements TypeLocator {

  private StandardTypeLocator locator;

  public CommonTypeLocator() {
    locator = new StandardTypeLocator();
    locator.registerImport("java.util");
    locator.registerImport("com.github.zzt93.syncer.common.data");
  }

  @Override
  public Class<?> findType(String typeName) throws EvaluationException {
    return locator.findType(typeName);
  }
}
