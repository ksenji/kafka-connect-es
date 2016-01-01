package org.apache.kafka.connect.es;

public abstract class Dcl<T> {
	private volatile T t;

	public abstract T init();

	public T get() {
		T local = t;
		if (local == null) {
			synchronized (this) {
				local = t;
				if (local == null) {
					t = local = init();
				}
			}
		}
		return local;
	}
}
