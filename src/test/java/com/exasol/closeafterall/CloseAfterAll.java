package com.exasol.closeafterall;

import java.lang.annotation.*;

/**
 * This annotation marks a resource that should be closed after all tests were executed.
 * <p>
 * In order to make this work you need to add the {@link CloseAfterAllExtension} to your test class.
 * </p>
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface CloseAfterAll {
}
