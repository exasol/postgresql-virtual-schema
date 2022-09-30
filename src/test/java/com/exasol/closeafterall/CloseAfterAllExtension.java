package com.exasol.closeafterall;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.exasol.errorreporting.ExaError;

/**
 * This JUnit extension closes resources in static fields that are annotated with {@code @CloseAfterAll}. You can use it
 * by annotating your class with {@code @ExtendWith({ CloseAfterAllExtension.class })}.
 */
public class CloseAfterAllExtension implements AfterAllCallback {

    @Override
    public void afterAll(final ExtensionContext extensionContext) {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final Field[] fields = testClass.getClass().getDeclaredFields();
        for (final Field field : fields) {
            closeField(testClass, field);
        }
    }

    private void closeField(final Class<?> testClass, final Field field) {
        if (field.isAnnotationPresent(CloseAfterAll.class)) {
            field.setAccessible(true);
            try {
                final Object annotatedObject = field.get(null);
                closeObject(field, annotatedObject);
            } catch (final IllegalAccessException | IOException e) {
                throw new IllegalStateException("Failed to close " + field.getName());
            }
        }
    }

    private void closeObject(final Field field, final Object annotatedObject) throws IOException {
        if (annotatedObject instanceof Closeable) {
            ((Closeable) annotatedObject).close();
        } else {
            throw new IllegalStateException(ExaError.messageBuilder("E-VSPG-9").message(
                    "Could not close the field {{field}} annotated with @CloseAfterAll since it does not implement Closable.")
                    .parameter("field", field.getName()).toString());
        }
    }
}
