package io.gameframe.storage;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
public interface DocumentStore extends AutoCloseable {
    CompletionStage<Optional<Snapshot>> load(StoreKey key);
    CompletionStage<WriteResult> write(StoreKey key, WriteCommand command);
    /** Barrier for earlier operations submitted to this key; not a multi-document transaction. */
    CompletionStage<Void> flush(StoreKey key);
    @Override void close();
}
