using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace BigMission.CommandTools; 

/// <summary>
/// Tools for encrypting and decrypting.
/// </summary>
public class KeyUtilities
{
    public static string EncodeToken(Guid appId, string apiKey, string aesKeyStr)
    {
        var aesKey = Encoding.UTF8.GetBytes(aesKeyStr);
        var tb = GetTokenBuffer(apiKey, appId);
        var r = Encrypt(tb, aesKey);
        return Convert.ToBase64String(r);
    }

    public static (Guid appId, string apiKey) DecodeToken(string token, string aesKeyStr)
    {
        var aesKey = Encoding.UTF8.GetBytes(aesKeyStr);
        var decodeBuff = Convert.FromBase64String(token);
        var dr = Decrypt(decodeBuff, aesKey);
        return GetTokenComponents(dr);
    }

    private static byte[] GetTokenBuffer(string apiKey, Guid appGuid)
    {
        var buff = new List<byte>();
        buff.AddRange(appGuid.ToByteArray());
        buff.AddRange(Encoding.UTF8.GetBytes(apiKey));
        return [.. buff];
    }

    private static (Guid appId, string apiKey) GetTokenComponents(byte[] tokenData)
    {
        var gb = tokenData[..16];
        var guid = new Guid(gb);
        var key = Encoding.UTF8.GetString(tokenData[16..]);
        return (guid, key);
    }

    // https://docs.microsoft.com/en-us/dotnet/api/system.security.cryptography.aes?view=net-6.0
    private static byte[] Encrypt(byte[] plainText, byte[] aesKey)
    {
        using Aes aesAlg = Aes.Create();
        aesAlg.Key = aesKey;

        ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);
        var encrypted = encryptor.TransformFinalBlock(plainText, 0, plainText.Length);
        var dataWithIV = new List<byte>(aesAlg.IV);
        dataWithIV.AddRange(encrypted);
        return [.. dataWithIV];
    }

    private static byte[] Decrypt(byte[] cipher, byte[] aesKey)
    {
        using Aes aesAlg = Aes.Create();
        aesAlg.Key = aesKey;
        aesAlg.IV = cipher[..16];
        cipher = cipher[16..];
        ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);
        return decryptor.TransformFinalBlock(cipher, 0, cipher.Length);
    }

    /// <summary>
    /// Generate a new API key.
    /// </summary>
    /// <returns></returns>
    public static string NewApiKey()
    {
        //var bytes = RandomNumberGenerator.GetBytes(25);
        var bytes = new byte[25];
        using (var crypto = new RNGCryptoServiceProvider())
            crypto.GetBytes(bytes);
        var base64 = Convert.ToBase64String(bytes);
        var result = Regex.Replace(base64, "[^A-Za-z0-9]", "");
        return result;
    }
}
