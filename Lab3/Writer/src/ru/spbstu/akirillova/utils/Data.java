package ru.spbstu.akirillova.utils;

public class Data {
    private byte[] array;
    private int numOfElements;

    private void Resize(int newSize) {
        byte[] arr = new byte[Math.max(newSize, 2 * numOfElements)];
        if (array != null)
            System.arraycopy(array, 0, arr, 0, numOfElements);
        else
            array = arr;
    }

    public void Clear() {
        numOfElements = 0;
    }

    public boolean IsEmpty() {
        return numOfElements == 0;
    }

    public byte[] ExtractBytes() {
        byte[] arr = new byte[numOfElements];
        System.arraycopy(array, 0, arr, 0, numOfElements);
        numOfElements = 0;
        return arr;
    }

    public short[] ExtractShorts() {
        short[] arr = new short[(numOfElements % 2 == 0) ? numOfElements / 2 : numOfElements / 2 + 1];
        for (int i = 0; i < numOfElements; i += 2)
            arr[i / 2] = (short) ((short)array[i] << 8 + ((numOfElements % 2 == 0) ? (short)array[i + 1] : 0));
        numOfElements = 0;
        return arr;
    }

    public char[] ExtractChars() {
        char[] arr = new char[numOfElements];
        for (int i = 0; i < numOfElements; i++)
            arr[i] = (char)array[i];
        numOfElements = 0;
        return arr;
    }

    public boolean PushBack(byte[] additionalData) {
        return PushBack(additionalData, additionalData.length);
    }
    public boolean PushBack(byte[] additionalData, int additionDataSize) {
        if (additionalData == null)
            return false;

        if (array == null)
            Resize(numOfElements + additionDataSize);
        else if (array.length - numOfElements < additionDataSize)
            Resize(numOfElements + additionDataSize);

        if (array != null)
            System.arraycopy(additionalData, 0, array, numOfElements, additionDataSize);
        numOfElements += additionDataSize;

        return true;
    }
}
