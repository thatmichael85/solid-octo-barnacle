package dist.migration.handlers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.CollectionName;
import dist.migration.dtos.DataBaseName;
import dist.migration.dtos.EventType;
import dist.migration.dtos.InputDto;
import dist.migration.services.MongoMigrationService;
import dist.migration.validators.InputValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;

class MigrationExecutorTest {

    @Mock
    private Configuration config;
    @Mock
    private InputValidator validator;
    @Mock
    private MongoMigrationService mongoMigrationService;

    @Mock
    private AppConfigProperties appConfigProperties;

    private MigrationExecutor executor;
    private InputDto inputDto;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        inputDto = new InputDto();
        inputDto.setEnv("test");
        inputDto.setEventType(EventType.EXECUTE_MIGRATION);
        inputDto.setCollectionName(CollectionName.COLLECTION_ONE);
        inputDto.setDataBaseName(DataBaseName.DEFAULT_DATA_BASE);
        executor = new MigrationExecutor(mongoMigrationService);
    }
    @Test
    void testRunStartsMigration() {
        // Define behavior for mocked methods
        when(mongoMigrationService.testSourceConnectivity()).thenReturn(Mono.just(true));
        when(mongoMigrationService.testDestinationConnectivity()).thenReturn(Mono.just(true));
        when(mongoMigrationService.migrate()).thenReturn(Mono.empty());

        assertDoesNotThrow(() -> executor.run());

        verify(mongoMigrationService).testSourceConnectivity();
        verify(mongoMigrationService).testDestinationConnectivity();
        verify(mongoMigrationService).migrate();
    }

    @Test
    void testRunHandlesExceptionInDestinationConnectivity() {
        when(mongoMigrationService.testSourceConnectivity()).thenReturn(Mono.just(true));
        when(mongoMigrationService.testDestinationConnectivity()).thenReturn(Mono.just(false));
        assertThrows(MigrationExecutorException.class, () -> executor.run());

        verify(mongoMigrationService).testSourceConnectivity();
        verify(mongoMigrationService).testDestinationConnectivity();
        verify(mongoMigrationService, never()).migrate();
    }

    @Test
    void testRunHandlesExceptionInSourceConnectivity() {
        when(mongoMigrationService.testSourceConnectivity()).thenReturn(Mono.just(false));
        assertThrows(MigrationExecutorException.class, () -> executor.run());

        verify(mongoMigrationService).testSourceConnectivity();
        verify(mongoMigrationService, never()).testDestinationConnectivity();
        verify(mongoMigrationService, never()).migrate();
    }


    @Test
    void testRunHandlesExceptionInMigration() {
        when(mongoMigrationService.testSourceConnectivity()).thenReturn(Mono.just(true));
        when(mongoMigrationService.testDestinationConnectivity()).thenReturn(Mono.just(true));
        when(mongoMigrationService.migrate()).thenReturn(Mono.error(new RuntimeException("Migration failed")));

        MigrationExecutorException e = assertThrows(MigrationExecutorException.class, () -> executor.run());

        verify(mongoMigrationService).testSourceConnectivity();
        verify(mongoMigrationService).testDestinationConnectivity();
        verify(mongoMigrationService).migrate();
    }
}
