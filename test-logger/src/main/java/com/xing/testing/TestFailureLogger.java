package com.xing.testing;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.ErrorStatus;
import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestListener;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestIdentifier;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class TestFailureLogger<E> extends UnsynchronizedAppenderBase<E>
    implements TestListener,
        TestExecutionListener,
        org.junit.platform.launcher.TestExecutionListener {

  private static class LogSegment {
    Boolean log = false;
    List<Object> entries = new ArrayList<>(10);
  }

  private static final Hashtable<Long, LogSegment> log = new Hashtable<>();

  private static final ThreadLocal<Long> currentTest = new ThreadLocal<>();

  private static TestFailureLogger appender;

  protected Encoder<E> encoder;

  public Encoder<E> getEncoder() {
    return encoder;
  }

  public void setEncoder(Encoder<E> encoder) {
    this.encoder = encoder;
    appender = this;
  }

  /**
   * Checks that requires parameters are set and if everything is in order, activates this appender.
   */
  public void start() {
    int errors = 0;
    if (this.encoder == null) {
      addStatus(new ErrorStatus("No encoder set for the appender named \"" + name + "\".", this));
      errors++;
    }

    // only error free appenders should be activated
    if (errors == 0) {
      super.start();
    }
  }

  @Override
  protected void append(E eventObject) {
    LogSegment l = log.get(currentTest.get());
    if (l != null) {
      l.entries.add(eventObject);
    }
  }

  @Override
  public void addError(Test test, Throwable e) {
    LogSegment l = log.get((long) test.hashCode());
    if (l != null) {
      l.log = true;
    }
  }

  @Override
  public void addFailure(Test test, AssertionFailedError e) {
    LogSegment l = log.get((long) test.hashCode());
    if (l != null) {
      l.log = true;
    }
  }

  @Override
  public void endTest(Test test) {
    LogSegment l = log.remove((long) test.hashCode());
    currentTest.remove();
    if (l != null && l.log) {
      for (Object e : l.entries) {
        try {
          System.out.write(this.encoder.encode((E) e));
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
  }

  @Override
  public void startTest(Test test) {
    Long th = (long) test.hashCode();
    log.putIfAbsent(th, new LogSegment());
    currentTest.set(th);
  }

  @Override
  public void executionStarted(TestIdentifier test) {
    Long th = (long) test.hashCode();
    log.putIfAbsent(th, new LogSegment());
    currentTest.set(th);
  }

  @Override
  public void executionFinished(TestIdentifier test, TestExecutionResult result) {
    LogSegment l = log.remove((long) test.hashCode());
    currentTest.remove();
    if (l != null && result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
      for (Object e : l.entries) {
        try {
          System.out.write(appender.encoder.encode((E) e));
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
  }

  /**
   * Prepares the {@link Object test instance} of the supplied {@link TestContext test context}, for
   * example by injecting dependencies.
   *
   * <p>This method should be called immediately after instantiation of the test instance but prior
   * to any framework-specific lifecycle callbacks.
   *
   * <p>The default implementation is <em>empty</em>. Can be overridden by concrete classes as
   * necessary.
   *
   * @param testContext the test context for the test; never {@code null}
   * @throws Exception allows any exception to propagate
   */
  @Override
  public void prepareTestInstance(TestContext testContext) throws Exception {
    Long th = springTestKey(testContext);
    LogSegment l = log.putIfAbsent(th, new LogSegment());
    currentTest.set(th);
  }

  /**
   * Pre-processes a test <em>immediately before</em> execution of the {@link Method test method} in
   * the supplied {@link TestContext test context} &mdash; for example, for timing or logging
   * purposes.
   *
   * <p>This method <strong>must</strong> be called after framework-specific <em>before</em>
   * lifecycle callbacks.
   *
   * <p>The default implementation is <em>empty</em>. Can be overridden by concrete classes as
   * necessary.
   *
   * @param test the test context in which the test method will be executed; never {@code null}
   * @throws Exception allows any exception to propagate
   * @see #beforeTestMethod
   * @see #afterTestMethod
   * @see #afterTestExecution
   * @since 5.0
   */
  @Override
  public void beforeTestExecution(TestContext test) throws Exception {
    Long th = springTestKey(test);
    LogSegment l = log.putIfAbsent(th, new LogSegment());
    currentTest.set(th);
  }

  /**
   * Post-processes a test <em>immediately after</em> execution of the {@link Method test method} in
   * the supplied {@link TestContext test context} &mdash; for example, for timing or logging
   * purposes.
   *
   * <p>This method <strong>must</strong> be called before framework-specific <em>after</em>
   * lifecycle callbacks.
   *
   * <p>The default implementation is <em>empty</em>. Can be overridden by concrete classes as
   * necessary.
   *
   * @param test the test context in which the test method will be executed; never {@code null}
   * @throws Exception allows any exception to propagate
   * @see #beforeTestMethod
   * @see #afterTestMethod
   * @see #beforeTestExecution
   * @since 5.0
   */
  @Override
  public void afterTestExecution(TestContext test) throws Exception {
    Long th = springTestKey(test);
    currentTest.remove();
    LogSegment l = log.remove(th);
    if (l != null && test.getTestException() != null) {
      for (Object e : l.entries) {
        try {
          System.out.write(appender.encoder.encode((E) e));
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
  }

  private long springTestKey(TestContext testContext) {
    return testContext.getTestInstance().hashCode();
  }
}
