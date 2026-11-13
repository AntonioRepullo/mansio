package com.mansio.incident;

import org.junit.Test;

class IncidentStatusTest {

    @Test
    void open_canMoveToInProgress() {
        assertThat(IncidentStatus.OPEN.canTransitionTo(IncidentStatus.IN_PROGRESS)).isTrue();
    }

    @Test
    void open_cannotMoveDirectlyToResolved() {
        assertThat(IncidentStatus.OPEN.canTransitionTo(IncidentStatus.RESOLVED)).isFalse();
    }

    @Test
    void closed_isTerminal() {
        for (IncidentStatus target : IncidentStatus.values()) {
            assertThat(IncidentStatus.CLOSED.canTransitionTo(target)).isFalse();
        }
    }
}