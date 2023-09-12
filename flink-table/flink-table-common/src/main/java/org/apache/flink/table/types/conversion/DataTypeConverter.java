package org.apache.flink.table.types.conversion;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * An interface that converts between the internal Flink data structure and the conversion class of
 * a DataType.
 */
@PublicEvolving
public interface DataTypeConverter<I, E> extends Serializable {

    default void open(ClassLoader classLoader) {
        assert classLoader != null;
        // nothing to do
    }

    /**
     * Converts to internal data structure.
     *
     * <p>Note: Parameter must not be null. Output must not be null.
     */
    I toInternal(E external);

    /**
     * Converts to internal data structure or {@code null}.
     *
     * <p>The nullability could be derived from the data type. However, this method reduces null
     * checks.
     */
    default I toInternalOrNull(E external) {
        if (external == null) {
            return null;
        }
        return toInternal(external);
    }

    /**
     * Converts to external data structure.
     *
     * <p>Note: Parameter must not be null. Output must not be null.
     */
    E toExternal(I internal);

    /**
     * Converts to external data structure or {@code null}.
     *
     * <p>The nullability could be derived from the data type. However, this method reduces null
     * checks.
     */
    default E toExternalOrNull(I internal) {
        if (internal == null) {
            return null;
        }
        return toExternal(internal);
    }

    /**
     * Returns whether this conversion is a no-op.
     *
     * <p>An identity conversion means that the type is already an internal data structure.
     */
    default boolean isIdentityConversion() {
        return false;
    }
}
