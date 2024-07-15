// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class HistosumStateFactory implements AccumulatorStateFactory<HistosumState> {

    public HistosumStateFactory() {}

    @Override
    public HistosumState createSingleState() {
        return new SingleHistosumState();
    }

    @Override
    public HistosumState createGroupedState() {
        return new GroupedHistosumState();
    }

    public static class GroupedHistosumState implements GroupedAccumulatorState, HistosumState {

        public GroupedHistosumState() {
            this.maps = new ArrayList<>();
        }

        private final ArrayList<Map<String, Double>> maps;
        private int groupId;

        @Override
        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size) {
            maps.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize() {
            return maps.size();
        }

        @Override
        public Map<String, Double> getMap() {
            return maps.get(groupId);
        }

        @Override
        public void setMap(Map<String, Double> m) {
            maps.ensureCapacity(groupId);
            maps.set(groupId, m);
        }

    }

    public static class SingleHistosumState implements HistosumState {

        public SingleHistosumState() {
            this.map = new LinkedHashMap<>();
        }

        private Map<String, Double> map;

        @Override
        public Map<String, Double> getMap() {
            return map;
        }

        @Override
        public void setMap(Map<String, Double> m) {
            this.map = m;
        }

        @Override
        public long getEstimatedSize() {
            return map == null ? 0L : map.size();
        }

    }

}
