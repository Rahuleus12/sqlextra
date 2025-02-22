-- SQL function to update columns from one table to another from different databases for Microsoft Express SQL 2014

CREATE FUNCTION UpdateColumnsFromDifferentDatabases()
RETURNS INT
AS
BEGIN
    DECLARE @RowsAffected INT = 0;

    -- Assuming we have two tables, TableA in DatabaseA and TableB in DatabaseB
    -- We want to update columns from TableA to TableB

    BEGIN TRANSACTION;

    TRY
        -- Update operation
        UPDATE TableB
        SET Column1 = TableA.Column1,
            Column2 = TableA.Column2
        FROM [DatabaseA].[dbo].TableA
        INNER JOIN [DatabaseB].[dbo].TableB ON TableA.ID = TableB.ID;

        SET @RowsAffected = @@ROWCOUNT;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        -- Handle error
        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH

    RETURN @RowsAffected;
END;