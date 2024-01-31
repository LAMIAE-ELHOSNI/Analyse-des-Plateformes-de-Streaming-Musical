ALTER PROCEDURE dbo.TestDataWarehouse
    @ErrorCount INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ErrorMessage NVARCHAR(MAX);

    SET @ErrorCount = 0;

    -- Test for null values in users_data table
    IF EXISTS (SELECT 1 FROM users_data WHERE Age IS NULL)
    BEGIN
        SET @ErrorMessage = 'Test Failed: Null values found in users_data.Age column.';
        PRINT @ErrorMessage;
        SET @ErrorCount = @ErrorCount + 1;
    END;

    -- Test for ages under 16 in users_data table
    IF EXISTS (SELECT 1 FROM users_data WHERE Age < 16 AND Age IS NOT NULL)
    BEGIN
        SET @ErrorMessage = 'Test Failed: Ages under 16 found in users_data.Age column.';
        PRINT @ErrorMessage;
        SET @ErrorCount = @ErrorCount + 1;
    END;

    -- If no errors found, print success message
    IF @ErrorCount = 0
    BEGIN
        PRINT 'All tests passed successfully.';
    END;
END;
