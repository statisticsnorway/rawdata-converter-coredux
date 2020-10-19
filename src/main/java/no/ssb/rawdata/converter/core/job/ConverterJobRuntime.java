package no.ssb.rawdata.converter.core.job;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static no.ssb.rawdata.converter.core.job.ConverterJobRuntime.State.NEW;
import static no.ssb.rawdata.converter.core.job.ConverterJobRuntime.State.PAUSED;
import static no.ssb.rawdata.converter.core.job.ConverterJobRuntime.State.STARTED;
import static no.ssb.rawdata.converter.core.job.ConverterJobRuntime.State.STOPPED;

class ConverterJobRuntime {
    enum State {
        NEW, STARTED, PAUSED, STOPPED;
    }

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private State state = NEW;

    public State getState() {
        return state;
    }

    public long getElapsedTimeInSeconds() {
        return stopwatch.elapsed(TimeUnit.SECONDS);
    }

    public String getElapsedTimeAsString() {
        return stopwatch.elapsed().toString().substring(2);
    }

    public void start() {
        checkState(isStartable(), "Cannot start from " + state + " state");
        state = STARTED;
        if (! stopwatch.isRunning()) {
            stopwatch.start();
        }
    }

    public void pause() {
        checkState(isPauseable(), "Cannot pause from " + state + " state");
        state = PAUSED;
        if (stopwatch.isRunning()) {
            stopwatch.stop();
        }
    }

    public void stop() {
        checkState(isStoppable(), "Cannot stop from " + state + " state");
        state = STOPPED;
        if (stopwatch.isRunning()) {
            stopwatch.stop();
        }
    }

    public boolean isStartable() {
        return state == NEW;
    }

    public boolean isPauseable() {
        return state == STARTED;
    }

    public boolean isStoppable() {
        return state != STOPPED;
    }

    public boolean isStarted() {
        return state == STARTED;
    }

    public boolean isPaused() {
        return state == PAUSED;
    }

    public boolean isStopped() {
        return state == STOPPED;
    }

}
