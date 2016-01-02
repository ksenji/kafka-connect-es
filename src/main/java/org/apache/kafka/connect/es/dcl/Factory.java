package org.apache.kafka.connect.es.dcl;

import java.util.function.Supplier;

public class Factory {

	public static <T> Dcl<T> of(Supplier<T> supp) {
		return new Dcl<T>() {
			@Override
			public T init() {
				return supp.get();
			}
		};
	}

	public static abstract class Dcl<T> {
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
}
