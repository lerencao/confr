package org.confr.storage;

public class  ValueChangedEvent {
    public static ValueChangedEvent NotExist(String key) {
        return new ValueChangedEvent(key, Integer.MIN_VALUE, null);
    }

    public static ValueChangedEvent Updated(String key, int version, String data) {
        assert version > Integer.MIN_VALUE;
        return new ValueChangedEvent(key, version, data);
    }

    private final int version;
    private final String key;
    private final String data;
    private ValueChangedEvent(String key, int version, String data) {
        this.key = key;
        this.version = version;
        this.data = data;
    }

    public int getVersion() {
        return version;
    }

    public String getData() {
        return data;
    }

    public String getKey() {
        return key;
    }

    @Override public boolean equals(Object obj) {
        if (obj instanceof ValueChangedEvent) {
            return version == ((ValueChangedEvent) obj).version && data.equals(((ValueChangedEvent) obj).data);
        } else {
            return super.equals(obj);
        }
    }

}
