namespace MDR_Coder;

public interface ITestingDataLayer
{
    int EstablishExpectedData();
    void TransferTestSDData(Source source);
    IEnumerable<int> ObtainTestSourceIDs();
}

