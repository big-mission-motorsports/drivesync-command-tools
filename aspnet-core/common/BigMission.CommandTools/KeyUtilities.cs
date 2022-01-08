using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Tools for encrypting and decrypting.
    /// </summary>
    public class KeyUtilities
    {
        private static readonly byte[] AES_KEY = Encoding.UTF8.GetBytes("439ekdaaqwewwwwmgjhk234dsmlapepo");

        public static string EncodeToken(Guid appId, string apiKey)
        {
            var tb = GetTokenBuffer(apiKey, appId);
            var r = Encrypt(tb);
            return Convert.ToBase64String(r);
        }

        public static (Guid appId, string apiKey) DecodeToken(string token)
        {
            var decodeBuff = Convert.FromBase64String(token);
            var dr = Decrypt(decodeBuff);
            return GetTokenComponents(dr);
        }

        private static byte[] GetTokenBuffer(string apiKey, Guid appGuid)
        {
            var buff = new List<byte>();
            buff.AddRange(appGuid.ToByteArray());
            buff.AddRange(Encoding.UTF8.GetBytes(apiKey));
            return buff.ToArray();
        }

        private static (Guid appId, string apiKey) GetTokenComponents(byte[] tokenData)
        {
            var gb = tokenData[..16];
            var guid = new Guid(gb);
            var key = Encoding.UTF8.GetString(tokenData[16..]);
            return (guid, key);
        }

        // https://docs.microsoft.com/en-us/dotnet/api/system.security.cryptography.aes?view=net-6.0
        private static byte[] Encrypt(byte[] plainText)
        {
            using Aes aesAlg = Aes.Create();
            aesAlg.Key = AES_KEY;

            ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);
            var encrypted = encryptor.TransformFinalBlock(plainText, 0, plainText.Length);
            var dataWithIV = new List<byte>(aesAlg.IV);
            dataWithIV.AddRange(encrypted);
            return dataWithIV.ToArray();
        }

        private static byte[] Decrypt(byte[] cipher)
        {
            using Aes aesAlg = Aes.Create();
            aesAlg.Key = AES_KEY;
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
}
