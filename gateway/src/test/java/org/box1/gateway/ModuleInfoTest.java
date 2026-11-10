package org.box1.gateway;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModuleInfoTest {
    @Test
    void describesThisModuleAndItsBundlePath() {
        ModuleInfo info = ModuleInfo.current();
        assertEquals("site-gateway", info.repository());
        assertEquals("gateway", info.module());
        assertEquals("/gateway/quarkus-app/", info.bundlePath());
        assertTrue(info.javaVersion() >= 21);
    }

    @Test
    void rejectsMissingIdentityOrAnOldJava() {
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo(" ", "gateway", 21));
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo("site-gateway", null, 21));
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo("site-gateway", "gateway", 17));
    }
}
