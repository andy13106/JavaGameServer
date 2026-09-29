package io.gameframe.scene;
import io.gameframe.base.Factory;
import java.util.*;
import java.util.function.Function;

/** Configuration-to-component construction. ObjectManager attaches the result on its actor. */
public final class ComponentFactory {
    public record Spec(int typeId, Map<String, String> properties) {
        public Spec {
            if (typeId < 256 || typeId > 65535) throw new IllegalArgumentException("component type ID");
            properties = Map.copyOf(properties);
        }
        public Spec(int typeId) { this(typeId, Map.of()); }
    }
    private final Factory<Integer, Function<Spec, ? extends GameComponent>> creators = new Factory<>();
    public boolean register(int typeId, Function<Spec, ? extends GameComponent> creator) {
        new Spec(typeId); Objects.requireNonNull(creator);
        return creators.register(typeId, () -> creator);
    }
    public GameComponent create(Spec spec) {
        Objects.requireNonNull(spec);
        var creator = creators.create(spec.typeId()).orElseThrow(() -> new IllegalArgumentException("unknown component " + spec.typeId()));
        var component = Objects.requireNonNull(creator.apply(spec), "component factory returned null");
        if (component.typeId() != spec.typeId() || !component.detached())
            throw new IllegalStateException("factory returned wrong type or an attached component");
        return component;
    }
}
