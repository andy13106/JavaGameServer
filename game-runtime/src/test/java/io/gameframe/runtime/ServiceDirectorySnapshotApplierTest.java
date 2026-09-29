package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectorySnapshotApplierTest {
    @Test
    void appliesRemoteSnapshotAndReportsFencingOutcomes() {
        var view = new ServiceDirectoryView();
        long now = System.currentTimeMillis();
        var registration = new ServiceDirectory.Registration("game", "one", URI.create("http://127.0.0.1:19001"),
                Set.of("rpc"), 1, 60_000, 1);
        var lease = new ServiceDirectory.Lease("game", "one", 1, 11);
        var first = ServiceDirectorySnapshotApplier.apply(view,
                List.of(new ServiceDirectoryView.Observation(registration, lease, now + 60_000, 2, false)), now);
        assertEquals(1, first.total());
        assertEquals(1, first.accepted());
        var fenced = ServiceDirectorySnapshotApplier.apply(view,
                List.of(new ServiceDirectoryView.Observation(registration,
                        new ServiceDirectory.Lease("game", "one", 1, 22), now + 60_000, 2, false)), now);
        assertEquals(1, fenced.outcomes().get(ServiceDirectoryView.UpdateResult.FENCED));
        assertEquals(1, view.size());
    }

    @Test
    void rejectsExpiredSnapshotWithoutChangingView() {
        var view = new ServiceDirectoryView();
        long now = System.currentTimeMillis();
        var registration = new ServiceDirectory.Registration("game", "expired", URI.create("http://127.0.0.1:19002"),
                Set.of("rpc"), 1, 60_000, 1);
        var result = ServiceDirectorySnapshotApplier.apply(view,
                List.of(new ServiceDirectoryView.Observation(registration,
                        new ServiceDirectory.Lease("game", "expired", 1, 33), now, 0, false)), now);
        assertEquals(1, result.outcomes().get(ServiceDirectoryView.UpdateResult.EXPIRED));
        assertEquals(0, view.size());
    }
}