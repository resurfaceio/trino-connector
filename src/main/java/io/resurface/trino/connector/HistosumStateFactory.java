// © 2016-2024 Graylog, Inc.

package io.resurface.trino.connector;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

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
            this.maps = new ObjectBigArray<>();
        }

        private final ObjectBigArray<Map<String, Object>> maps;
        private long groupId;

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
            return maps.sizeOf();
        }

        @Override
        public Map<String, Object> getMap() {
            return maps.get(groupId);
        }

        @Override
        public void setMap(Map<String, Object> newMap) {
            maps.ensureCapacity(groupId);
            maps.set(groupId, newMap);
        }

    }

    public static class SingleHistosumState implements HistosumState {

        public SingleHistosumState() {
            this.stateMap = new LinkedHashMap<>();
        }

        private Map<String, Object> stateMap;

        @Override
        public Map<String, Object> getMap() {
            return stateMap;
        }

        @Override
        public void setMap(Map<String, Object> newMap) {
            this.stateMap = newMap;
        }

        @Override
        public long getEstimatedSize() {
            if (this.stateMap != null) {
                return this.stateMap.size();
            } else {
                return 0L;
            }
        }

    }

}
