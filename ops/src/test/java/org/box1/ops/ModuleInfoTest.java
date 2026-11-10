package org.box1.ops;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModuleInfoTest {
    @Test
    void describesThisModuleAndItsBundlePath() {
        ModuleInfo info = ModuleInfo.current();
        assertEquals("site-gateway", info.repository());
        assertEquals("ops", info.module());
        assertEquals("/ops/quarkus-app/", info.bundlePath());
        assertTrue(info.javaVersion() >= 21);
    }

    @Test
    void rejectsMissingIdentityOrAnOldJava() {
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo(" ", "ops", 21));
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo("site-gateway", null, 21));
        assertThrows(IllegalArgumentException.class, () -> new ModuleInfo("site-gateway", "ops", 17));
    }
}
