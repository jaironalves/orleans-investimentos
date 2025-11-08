using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Investimentos.Silo.Serialization;

public class LegacyConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
    {
        bool hasLegacyProps = typeToConvert.GetProperties()
            .Any(p => Attribute.IsDefined(p, typeof(JsonPropertyNamesLegacyAttribute)));

        bool isNewtonsoftLegacy = Attribute.IsDefined(typeToConvert, typeof(JsonNewtonsoftLegacyAttribute));

        return hasLegacyProps || isNewtonsoftLegacy;
    }

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var converterType = typeof(LegacyConverter<>).MakeGenericType(typeToConvert);
        return (JsonConverter)Activator.CreateInstance(converterType)!;
    }
}   
