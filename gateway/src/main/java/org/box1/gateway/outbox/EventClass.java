package org.box1.gateway.outbox;

/** What an outbox row carries. Readings may be dropped by the buffer bound; site events never are. */
public enum EventClass {
    READING, SITE_EVENT
}
